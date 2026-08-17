"""
Cerebro HTF para fondeo: entrada M1 + filtro de contexto HTF (15m/1h).
Estrategia separada de src/order_flow_signal.py (el "cerebro principal") -
no lo modifica ni depende de él salvo por reutilizar orderflow_data.py.

QUÉ ES REAL Y QUÉ ES APROXIMADO (mismo criterio que orderflow_data.py y
snapshot_engine.py, del cual se portan los detectores):
- ABSORPTION, STOP_RUN, INITIATIVE_PULLBACK: mismas fórmulas reales que
  snapshot_engine.py, adaptadas para operar sobre klines agregados en vez de
  estado de un motor en vivo (funcionan igual porque esas 3 fórmulas ya sólo
  usan high/low/close/volume/delta por vela, nunca tick-by-tick).
- BREAKOUT_VOL: la firma real exige "imbalance en la dirección de la
  ruptura" (requiere tick data, no disponible en klines) - acá se aproxima
  con el signo del delta de la vela, documentado explícitamente.
- ICEBERG / IMBALANCE_STACK: NO SE CALCULAN. Requieren refresh de order
  book L2 y imbalances 3:1 por nivel de precio - no son derivables de OHLCV
  agregado. Se omiten en vez de inventarse (mismo criterio que
  orderflow_data.build_ltf_candles).
- DELTA_DIV: se reutiliza orderflow_data._mark_delta_divergence tal cual.

decide() recibe contexto YA PRECALCULADO por backtest_htf.py (rolling
stats vectorizadas en pandas) - así decide() en sí mismo queda en simples
comparaciones de umbral, <1ms, sin cómputo pesado en el hot path.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class HTFParams:
    """"Opción C": más selectivo que la versión inicial (conviction 7->8,
    zona más ajustada 0.15%->0.10%, contra-tendencia más exigente 8->9,
    breakeven más rápido/ajustado 0.8/0.3->0.5/0.2 ATR) - resultado de la
    primera corrida walk-forward (WR 38%, PF 2.0, 212 trades/mes: exceso de
    señales de baja calidad, se busca menos cantidad y mejor selección."""
    sl_atr_mult: float = 1.2
    tp1_atr_mult: float = 2.5   # fallback si no hay zona HTF real en la dirección (ver _calc_structural_tps)
    tp2_atr_mult: float = 5.0
    tp1_atr_cap_mult: float = 3.0   # tope: aunque la zona real esté más lejos, no perseguir más de esto
    tp2_atr_cap_mult: float = 6.0
    tp1_size_pct: float = 0.6
    be_trigger_atr_mult: float = 0.5
    be_buffer_atr_mult: float = 0.2
    zone_cooldown_min: dict = field(default_factory=lambda: {
        "POC": 120, "VAH": 90, "VAL": 90, "SWING_H4": 180, "VWAP_15M": 60,
    })
    min_conviction: int = 8
    zone_proximity_pct: float = 0.0010
    max_hold_hours: float = 24.0
    counter_trend_conviction: int = 9

    # Filtro de regimen: bloquea señales cuando el ATR14_15m actual esta
    # comprimido (percentil bajo) contra su propia historia de 30 dias --
    # validado 2026-08-16 contra datos reales ago 2026: la ventana mala del
    # 4-15 ago (34 trades/PF 1.31 sin filtro) tenia ATR/precio ~44% mas bajo
    # que la mediana del periodo completo. Con este filtro esa misma ventana
    # pasa a PF ~2.85, y el periodo completo NO empeora (incluso mejora
    # levemente) -- no es solo "operar menos en rango", es que un ATR
    # comprimido hace que el SL (escalado a ese mismo ATR chico) quede
    # angosto justo cuando el precio sigue barriendo zonas HTF con el ruido
    # normal de siempre, generando SL-hits que en un regimen de volatilidad
    # normal no pasarian. None (sin historia suficiente todavia, ver
    # snapshot_engine.py) no bloquea nada -- fail-open, no fail-closed.
    atr_compression_percentile: float = 0.25

    # Filtro de costos reales (ver config/assets.py) - un TP1 más cerca que
    # esto casi seguro lo come el spread+slippage antes de llegar, y un R:R
    # neto por debajo de esto no compensa el costo de operar tan seguido.
    min_tp1_spread_mult: float = 3.0
    min_rr_net: float = 1.3

    # Detectores kline (ver docstring del módulo) - mismos umbrales reales
    # que config/constants.py, repetidos acá para que este archivo no
    # dependa de import cruzado del cerebro principal.
    absorption_percentile: float = 95.0
    absorption_flip_ratio: float = 0.35
    breakout_range_pct: float = 0.003
    breakout_vol_mult: float = 2.0
    pullback_htf_distance_pct: float = 0.003
    pullback_delta_exhaustion_pct: float = 0.3


_PRIORIDAD_CONVICTION = {1: 9, 2: 8, 3: 7, 4: 7}  # prioridad -> conviction base (sin iceberg/imbalance_stack)


def _evaluar_absorcion(vela: dict, vol_p95_2h: float) -> bool:
    """Misma fórmula que snapshot_engine._evaluar_absorcion: percentil de
    volumen >=95 sobre 2hs + flip ratio + rango chico (proxy de stall de
    precio) - todo derivable de OHLCV+delta por vela."""
    rango = vela["high"] - vela["low"]
    if rango <= 0 or vela["volume"] <= 0:
        return False
    if vela["volume"] < vol_p95_2h:
        return False
    flip_ratio = abs(vela["delta"]) / vela["volume"]
    if flip_ratio <= 0.35:
        return False
    if rango >= vela["close"] * 0.001:
        return False
    return True


def _evaluar_stop_run(vela: dict, high_20: float, low_20: float) -> Optional[str]:
    """Misma fórmula que snapshot_engine._evaluar_stop_run: sweep del
    high/low de las últimas 20 velas con reclaim + delta a favor de la
    reversión."""
    if high_20 is None or low_20 is None:
        return None
    if vela["high"] > high_20 and vela["close"] <= high_20 and vela["delta"] < 0:
        return "SHORT"
    if vela["low"] < low_20 and vela["close"] >= low_20 and vela["delta"] > 0:
        return "LONG"
    return None


def _evaluar_initiative_pullback(vela: dict, htf: dict, params: HTFParams, avg_abs_delta_10: float) -> Optional[str]:
    """Misma fórmula que snapshot_engine._evaluar_initiative_pullback:
    pullback a zona de valor HTF con delta contra-tendencia agotándose."""
    poc, vah, val = htf.get("poc"), htf.get("vah"), htf.get("val")
    if not (poc and vah and val):
        return None
    precio = vela["close"]
    if not any(nivel and precio > 0 and abs(precio - nivel) / precio < params.pullback_htf_distance_pct for nivel in (poc, vah, val)):
        return None

    tendencia = htf.get("cvd_trend")
    if tendencia not in ("UP", "DOWN"):
        return None
    direccion = "LONG" if tendencia == "UP" else "SHORT"

    es_pullback = (direccion == "LONG" and vela["delta"] < 0) or (direccion == "SHORT" and vela["delta"] > 0)
    if not es_pullback:
        return None

    if not avg_abs_delta_10 or avg_abs_delta_10 <= 0:
        return None
    if abs(vela["delta"]) > params.pullback_delta_exhaustion_pct * avg_abs_delta_10:
        return None

    return direccion


def _evaluar_breakout_vol(vela: dict, params: HTFParams, range_high_10: float, range_low_10: float, vol_prom_10: float) -> Optional[str]:
    """Aproximación de snapshot_engine._evaluar_breakout_vol: consolidación
    + volumen 2x + dirección. La firma real exige imbalance tick-a-tick en
    la dirección de la ruptura - no disponible en klines, se usa el signo
    del delta de la vela como proxy (documentado, no inventado como real)."""
    if range_high_10 is None or range_low_10 is None or vela["close"] <= 0:
        return None
    rango_pct = (range_high_10 - range_low_10) / vela["close"]
    if rango_pct > params.breakout_range_pct:
        return None
    if not vol_prom_10 or vol_prom_10 <= 0 or vela["volume"] < params.breakout_vol_mult * vol_prom_10:
        return None
    if vela["delta"] == 0:
        return None
    return "LONG" if vela["delta"] > 0 else "SHORT"


def prioridad_setup(vela: dict, htf: dict, params: HTFParams) -> tuple:
    """Igual que order_flow_signal._prioridad_setup pero sin ICEBERG ni
    IMBALANCE_STACK (no calculables de klines agregados - ver docstring)."""
    if vela.get("stop_run"):
        return "STOP_RUN", vela["stop_run"], 1
    if vela.get("abs"):
        return "ABSORPTION", ("LONG" if vela["delta"] < 0 else "SHORT"), 2
    if vela.get("div"):
        return "DELTA_DIV", ("LONG" if vela["delta"] < 0 else "SHORT"), 3
    if vela.get("initiative_pullback"):
        return "INITIATIVE_PULLBACK", vela["initiative_pullback"], 4
    if vela.get("breakout_vol"):
        return "BREAKOUT_VOL", vela["breakout_vol"], 4
    return None, None, 99


def _zonas_htf(htf: dict) -> list:
    zonas = [
        ("POC", htf.get("poc")), ("VAH", htf.get("vah")), ("VAL", htf.get("val")),
        ("SWING_H4", htf.get("swing_h4_h")), ("SWING_H4", htf.get("swing_h4_l")),
        ("VWAP_15M", htf.get("vwap_15m")),
    ]
    return [(n, v) for n, v in zonas if v]


def calc_structural_tps(entry: float, direccion: str, htf: dict, atr: float, params: HTFParams) -> tuple:
    """TP1/TP2 estructural: el próximo nivel HTF real (POC/VAH/VAL/Swing
    H4/VWAP15m) en la dirección del trade, no un múltiplo arbitrario de ATR.
    Fallback a ATR×tp1_atr_mult/tp2_atr_mult si no hay zona real más allá
    del entry (por ejemplo, ya se está en el extremo del rango reciente),
    siempre capeado a ATR×tp1_atr_cap_mult/tp2_atr_cap_mult - perseguir un
    nivel real muy lejano no es mejor que no tener nivel."""
    niveles = sorted({v for _, v in _zonas_htf(htf)})
    if direccion == "LONG":
        objetivos = [n for n in niveles if n > entry * 1.0005]
        tp1_fallback = entry + atr * params.tp1_atr_mult
        tp2_fallback = entry + atr * params.tp2_atr_mult
        tp1 = objetivos[0] if objetivos else tp1_fallback
        tp2 = objetivos[1] if len(objetivos) > 1 else tp2_fallback
        tp1 = min(tp1, entry + atr * params.tp1_atr_cap_mult)
        tp2 = min(tp2, entry + atr * params.tp2_atr_cap_mult)
    else:
        objetivos = [n for n in reversed(niveles) if n < entry * 0.9995]
        tp1_fallback = entry - atr * params.tp1_atr_mult
        tp2_fallback = entry - atr * params.tp2_atr_mult
        tp1 = objetivos[0] if objetivos else tp1_fallback
        tp2 = objetivos[1] if len(objetivos) > 1 else tp2_fallback
        tp1 = max(tp1, entry - atr * params.tp1_atr_cap_mult)
        tp2 = max(tp2, entry - atr * params.tp2_atr_cap_mult)

    return tp1, tp2


def calc_net_levels(entry: float, direccion: str, tp1_raw: float, tp2_raw: float, sl_raw: float,
                     hold_hours_est: float, asset_cfg: dict) -> dict:
    """Ajusta entry/SL/TP por costos reales del activo (config/assets.py):
    spread + slippage en la entrada y en cada salida, más swap estimado
    según las horas de hold esperadas. Sin esto, un backtest puede mostrar
    edge que en la práctica el spread/swap se come antes de que llegue a
    materializarse - ver filtro min_tp1_spread_mult/min_rr_net en decide()."""
    spread = asset_cfg["spread_bps"] / 10000
    slip = asset_cfg["slippage_bps"] / 10000
    swap_bps_8h = asset_cfg["swap_long_bps_8h"] if direccion == "LONG" else asset_cfg["swap_short_bps_8h"]
    swap_cost = (swap_bps_8h / 10000) * (hold_hours_est / 8.0)

    if direccion == "LONG":
        entry_net = entry * (1 + spread + slip)
        sl_net = sl_raw * (1 - spread - slip)
        tp1_net = tp1_raw * (1 - spread - slip - swap_cost)
        tp2_net = tp2_raw * (1 - spread - slip - swap_cost)
    else:
        entry_net = entry * (1 - spread - slip)
        sl_net = sl_raw * (1 + spread + slip)
        tp1_net = tp1_raw * (1 + spread + slip + swap_cost)
        tp2_net = tp2_raw * (1 + spread + slip + swap_cost)

    cost_bps = (2 * (spread + slip) + abs(swap_cost)) * 10000
    return {"entry": entry_net, "sl": sl_net, "tp1": tp1_net, "tp2": tp2_net, "cost_bps": cost_bps}


class HTFFundingBrain:
    """Estado propio (cooldowns por zona) - instanciar una vez por corrida
    de backtest/ventana walk-forward, no compartir entre ventanas (evita
    que el cooldown de una ventana de test contamine la siguiente)."""

    def __init__(self, params: Optional[HTFParams] = None, asset_cfg: Optional[dict] = None):
        self.params = params or HTFParams()
        if asset_cfg is None:
            from config.assets import get_asset_config
            asset_cfg = get_asset_config("BTCUSDT")
        self.asset_cfg = asset_cfg
        self._zone_cooldowns: dict = {}  # "POC_64000" -> ts_expiry_ms

    def _zona_valida(self, precio: float, htf: dict, ts_ms: int) -> tuple:
        """Devuelve (en_zona, tipo_zona, motivo). Chequea proximidad Y
        cooldown (por tipo de zona, ver ZONE_COOLDOWN_MIN)."""
        if precio <= 0:
            return False, "", "FUERA_DE_ZONA"
        for nombre, nivel in _zonas_htf(htf):
            dist_pct = abs(precio - nivel) / precio
            if dist_pct > self.params.zone_proximity_pct:
                continue
            key = f"{nombre}_{nivel:.0f}"
            expiry = self._zone_cooldowns.get(key, 0)
            if ts_ms < expiry:
                return False, nombre, f"COOLDOWN_{nombre}"
            cooldown_min = self.params.zone_cooldown_min.get(nombre, 60)
            self._zone_cooldowns[key] = ts_ms + cooldown_min * 60 * 1000
            return True, nombre, f"ZONA_{nombre}"
        return False, "", "FUERA_DE_ZONA"

    def validate_htf(self, pattern_dir: str, price: float, htf: dict, ts_ms: int, conviction: int) -> tuple:
        """Sin logging, sin side effects salvo el cooldown (que es estado
        real del proceso, igual que _check_zone_cooldown en el cerebro
        principal). Devuelve (valido, motivo)."""
        en_zona, zone_type, motivo = self._zona_valida(price, htf, ts_ms)
        if not en_zona:
            return False, motivo

        tendencia = htf.get("cvd_trend")
        cvd_dir = "LONG" if tendencia == "UP" else "SHORT" if tendencia == "DOWN" else None
        if cvd_dir is not None and pattern_dir != cvd_dir:
            # Contra-tendencia: solo se permite con convicción alta Y en
            # POC/VAL (zona de "soporte fuerte") Y con el CVD girando (el
            # propio hecho de estar evaluando esto ya implica una zona
            # tocada - "cvd_turning" se aproxima con FLAT o con el patrón
            # siendo DELTA_DIV/ABSORPTION, que ya miden agotamiento).
            if conviction < self.params.counter_trend_conviction:
                return False, "COUNTER_TREND_CONVICTION_INSUFICIENTE"
            if zone_type not in ("POC", "VAL"):
                return False, "COUNTER_TREND_ZONA_DEBIL"

        return True, "HTF_OK"

    def decide(self, vela: dict, htf: dict, ts_ms: int) -> Optional[dict]:
        """vela: dict con close/high/low/delta/volume + campos precalculados
        por el backtest (abs/div/stop_run/initiative_pullback/breakout_vol).
        htf: dict con poc/vah/val/swing_h4_h/swing_h4_l/vwap_15m/cvd_trend/
        atr14_15m/atr_rank_30d (ver atr_compression_percentile en HTFParams)."""
        atr_rank_30d = htf.get("atr_rank_30d")
        if atr_rank_30d is not None and atr_rank_30d < self.params.atr_compression_percentile:
            return None

        setup, direccion, prioridad = prioridad_setup(vela, htf, self.params)
        if setup is None:
            return None

        conviction = _PRIORIDAD_CONVICTION.get(prioridad, 6)
        if conviction < self.params.min_conviction:
            return None

        precio = vela["close"]
        valido, motivo = self.validate_htf(direccion, precio, htf, ts_ms, conviction)
        if not valido:
            return None

        atr = htf.get("atr14_15m")
        if not atr or atr <= 0:
            return None

        entry = precio
        sl_raw = entry - atr * self.params.sl_atr_mult if direccion == "LONG" else entry + atr * self.params.sl_atr_mult
        tp1_raw, tp2_raw = calc_structural_tps(entry, direccion, htf, atr, self.params)

        # Hold estimado: distancia a TP1 en unidades de ATR, en velas de 15m
        # (cada vela "cuesta" ~atr/20 de movimiento típico) - solo para
        # estimar el costo de swap, no una promesa de tiempo real de hold.
        atr_por_vela_15m = max(atr / 20.0, atr * 0.01)
        velas_est = max(1.0, abs(tp1_raw - entry) / atr_por_vela_15m)
        hold_hours_est = min(velas_est * 0.25, self.params.max_hold_hours)

        niveles = calc_net_levels(entry, direccion, tp1_raw, tp2_raw, sl_raw, hold_hours_est, self.asset_cfg)
        entry_net, sl_net, tp1_net, tp2_net = niveles["entry"], niveles["sl"], niveles["tp1"], niveles["tp2"]

        # Filtro de costos reales: TP1 tiene que valer la pena por encima
        # del spread, y el R:R neto (ya con spread/slippage/swap adentro)
        # tiene que seguir siendo atractivo - ver HTFParams.
        min_tp1_dist = entry_net * (self.params.min_tp1_spread_mult * self.asset_cfg["spread_bps"] / 10000)
        if abs(tp1_net - entry_net) < min_tp1_dist:
            return None

        riesgo_net = abs(entry_net - sl_net)
        if riesgo_net <= 0:
            return None
        rr_net = abs(tp1_net - entry_net) / riesgo_net
        if rr_net < self.params.min_rr_net:
            return None

        htf_boost = 2 if (htf.get("cvd_trend") in ("UP", "DOWN") and (
            (direccion == "LONG" and htf["cvd_trend"] == "UP") or (direccion == "SHORT" and htf["cvd_trend"] == "DOWN")
        )) else 0

        return {
            "decision": direccion,
            "setup_type": setup,
            "conviction": min(conviction + htf_boost, 10),
            "entry_price": round(entry_net, 2),
            "stop_loss_price": round(sl_net, 2),
            "tp1_price": round(tp1_net, 2),
            "tp2_price": round(tp2_net, 2),
            "tp1_size_pct": self.params.tp1_size_pct,
            "atr14_15m": atr,
            "cost_bps": round(niveles["cost_bps"], 2),
            "hold_hours_est": round(hold_hours_est, 2),
            "rr_net": round(rr_net, 3),
            "management_notes": motivo,
        }
