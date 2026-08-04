"""Port de "EL SENSEI v3 - Scanner Institucional" (Pine Script v6) a Python.

FUENTE: el bloque `pine_script` pegado por el usuario en la conversacion
(`indicator("EL SENSEI v3 - Scanner Institucional", ...)`). Cada seccion de
este archivo cita el numero de seccion del Pine original en el comentario.

TRANSPARENCIA SOBRE QUE ES PORT EXACTO Y QUE NO: el bloque fuente que se
pego tenia varios tramos explicitamente resumidos en el propio Pine
("// visuals omitted for brevity", "// ... find min prob index -> remove
all arrays at idx", "// mitigation logic...", "// ... Grado logic ...",
"// ... Alerts..."). Para esos tramos NO hay codigo Pine literal para
traducir -- se implemento el comportamiento que describe el comentario,
marcado abajo como [RECONSTRUIDO]. El resto (calculos base, CHoCH, formula
F1-F4, gestion de OB, aprendizaje, tasaCont dinamica, gatillos 9b) es
traduccion directa de codigo Pine real, marcado [PORT EXACTO].

ACTUALIZACION: SnapshotEngine.bars_multi_tf()/closed_timeframes_this_close()
(agregados en la misma sesion que este archivo, ver snapshot_engine.py) ya
exponen velas OHLC reales de 15m/1h/4h/1D -- la limitacion de arriba esta
resuelta. `on_bar_close(symbol, tf_min, bar: BarData)` sigue con firma
explicita (bar viene de MarketSnapshot.get_last_closed_bar(tf_min)), pero
BarData ahora es la clase compartida de src/schemas/market.py, no una copia
local -- MarketSnapshot, SnapshotEngine y este generador hablan el mismo
tipo. La conexion real esta en SignalOrchestrator.process_market_tick()
(opcional, self.sensei_generator puede ser None).

Este archivo SI se puede probar de forma aislada y determinista: armar una
lista de barras OHLCV (CSV o lo que sea) y llamar on_bar_close() en orden --
ese es el criterio de aceptacion #1 del prompt (determinismo vs TradingView).
"""
from __future__ import annotations

import statistics
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Optional

from src.schemas.market import BarData
from src.schemas.sensei_zones import SenseiZone
from src.services.zone_cache import ZoneCache


# ----------------------------------------------------------------------
# Config (config/sensei.yaml -> SenseiConfig) -- seccion 1 del Pine
# ----------------------------------------------------------------------

@dataclass
class SenseiConfig:
    enabled: bool = True
    enabled_timeframes: tuple[int, ...] = (15, 60, 240, 1440)
    piv_len: int = 3
    bos_lookback: int = 20
    hh_ll_period: int = 70
    atr_len: int = 14
    disp_multiplier: float = 1.2
    require_disp: bool = False
    choch_window: int = 20
    historical_zones: int = 90
    max_ob_activos: int = 8
    min_prob_ob: int = 50
    cont_alta_th: float = 50.0
    cont_baja_th: float = 55.0
    min_risk_atr: float = 0.5
    w1: float = 0.35
    w2: float = 0.25
    w3: float = 0.25
    w4: float = 0.15
    fvg_enabled: bool = True
    max_fvg_activos: int = 10

    @classmethod
    def from_yaml_dict(cls, raw: dict) -> "SenseiConfig":
        """raw: el dict ya cargado de config/sensei.yaml (sensei_generator: {...})."""
        w = raw.get("weights") or {}
        fvg = raw.get("fvg") or {}
        return cls(
            enabled=raw.get("enabled", True),
            enabled_timeframes=tuple(raw.get("enabled_timeframes", (15, 60, 240, 1440))),
            piv_len=raw.get("piv_len", 3),
            bos_lookback=raw.get("bos_lookback", 20),
            hh_ll_period=raw.get("hh_ll_period", 70),
            atr_len=raw.get("atr_len", 14),
            disp_multiplier=raw.get("disp_multiplier", 1.2),
            require_disp=raw.get("require_disp", False),
            choch_window=raw.get("choch_window", 20),
            historical_zones=raw.get("historical_zones", 90),
            max_ob_activos=raw.get("max_ob_activos", 8),
            min_prob_ob=raw.get("min_prob_ob", 50),
            cont_alta_th=raw.get("cont_alta_th", 50.0),
            cont_baja_th=raw.get("cont_baja_th", 55.0),
            min_risk_atr=raw.get("min_risk_atr", 0.5),
            w1=w.get("w1", 0.35), w2=w.get("w2", 0.25), w3=w.get("w3", 0.25), w4=w.get("w4", 0.15),
            fvg_enabled=fvg.get("enabled", True),
            max_fvg_activos=fvg.get("max_fvg_activos", 10),
        )

# BarData: se importa de src.schemas.market (misma clase que usa
# MarketSnapshot/SnapshotEngine) -- ver import arriba. Antes habia una copia
# local con los mismos campos; se elimino para que los tres modulos hablen
# el mismo tipo, no dos compatibles-por-coincidencia.


# ----------------------------------------------------------------------
# Registros internos (no son SenseiZone -- eso es la SALIDA publica;
# esto es el estado de trabajo del generador). Seccion 6 y 7 del Pine.
# ----------------------------------------------------------------------

@dataclass
class OBRecord:
    zone_id: str
    top: float
    bottom: float
    is_bull: bool
    prob_final: int          # probFinal AL MOMENTO DE CREAR el OB -- obProbs en Pine, estatico
    bos_score: int
    touched: bool = False
    touch_bar: int = 0
    approach_d: float = -1.0  # obApproachD -- dATR la primera vez que se acerco


@dataclass
class FVGRecord:
    zone_id: str
    top: float
    bottom: float
    is_bull: bool
    target: float             # precio que mitiga el gap si el precio lo re-toca
    created_bar: int


@dataclass
class SenseiState:
    tf_min: int
    symbol: str

    # Buffers OHLC -- el generador mantiene su propia historia (ver
    # docstring del modulo: MarketSnapshot no trae barras multi-TF)
    opens: Deque[float] = field(default_factory=lambda: deque(maxlen=600))
    highs: Deque[float] = field(default_factory=lambda: deque(maxlen=600))
    lows: Deque[float] = field(default_factory=lambda: deque(maxlen=600))
    closes: Deque[float] = field(default_factory=lambda: deque(maxlen=600))
    bar_count: int = 0

    # Indicadores incrementales (seccion 2)
    ema_base: Optional[float] = None
    atr: Optional[float] = None
    _atr_seed_trs: list = field(default_factory=list)  # para sembrar el ATR con SMA de los primeros atr_len TR

    # Estructura (seccion 2b-3)
    swing_high: Optional[float] = None
    swing_low: Optional[float] = None
    struct_trend: int = 0
    ultimo_alineado: bool = False
    velas_sin_bos: int = 0
    bars_choch_up: int = 9999
    bars_choch_down: int = 9999
    prev_choch_up: bool = False
    prev_choch_down: bool = False
    max_validos: int = 0
    min_validos: int = 0

    # Piernas (seccion 4)
    piernas_alc: int = 0
    piernas_baj: int = 0

    # Memoria de aprendizaje (seccion 8) -- listas acotadas a historical_zones (array.shift en Pine)
    mem_hold: list = field(default_factory=list)
    mem_reach_atr: list = field(default_factory=list)

    # Calificacion (seccion 9)
    calif_prob: float = 50.0

    # Order blocks y FVG activos
    obs: list = field(default_factory=list)   # list[OBRecord]
    fvgs: list = field(default_factory=list)  # list[FVGRecord]


class SenseiZoneGenerator:
    def __init__(self, zone_cache: ZoneCache, config: SenseiConfig) -> None:
        self.zone_cache = zone_cache
        self.cfg = config
        self.state: dict[tuple[str, int], SenseiState] = {}

    def _get_state(self, symbol: str, tf_min: int) -> SenseiState:
        key = (symbol, tf_min)
        if key not in self.state:
            self.state[key] = SenseiState(tf_min=tf_min, symbol=symbol)
        return self.state[key]

    def get_fvgs(self, symbol: str, tf_min: int) -> list[FVGRecord]:
        """FVGs activos (no mitigados) de (symbol, tf_min) -- seccion 7 del
        Pine, calculados en on_bar_close() pero NUNCA publicados a
        zone_cache (a diferencia de los OB, ver el loop `for ob in st.obs:
        self.zone_cache.upsert_zone(...)` mas abajo, que no tiene
        equivalente para FVG). Getter de solo lectura, aditivo -- no cambia
        nada de la deteccion/mitigacion ya validada, solo la expone."""
        st = self.state.get((symbol, tf_min))
        return list(st.fvgs) if st is not None else []

    # ------------------------------------------------------------------
    # on_bar_close -- TODA la logica de barra cerrada del Pine (secciones
    # 2 a 9b). Firma explicita (symbol, tf_min, bar) -- ver docstring del
    # modulo sobre por que NO recibe MarketSnapshot directamente.
    # ------------------------------------------------------------------
    def on_bar_close(self, symbol: str, tf_min: int, bar: BarData) -> None:
        cfg = self.cfg
        st = self._get_state(symbol, tf_min)

        st.opens.append(bar.open)
        st.highs.append(bar.high)
        st.lows.append(bar.low)
        st.closes.append(bar.close)
        st.bar_count += 1

        # necesitamos al menos 3 velas para high[2]/low[2] (FVG) y varias
        # mas para los lookbacks -- si no hay suficiente historia todavia,
        # solo acumulamos buffer y salimos (equivalente a que en Pine los
        # primeros N valores de una serie sean `na`).
        if len(st.closes) < 3:
            return

        closes, highs, lows, opens = list(st.closes), list(st.highs), list(st.lows), list(st.opens)
        close, open_, high, low = closes[-1], opens[-1], highs[-1], lows[-1]
        close1, open1, high1, low1 = closes[-2], opens[-2], highs[-2], lows[-2]
        high2, low2 = highs[-3], lows[-3]

        # ---- 2. CALCULOS BASE [PORT EXACTO] ----
        st.ema_base = self._update_ema(st.ema_base, close, period=20)
        alcista = close > st.ema_base if st.ema_base is not None else True
        bajista = not alcista

        tr = max(high - low, abs(high - close1), abs(low - close1))
        st.atr = self._update_atr(st, tr, period=cfg.atr_len)
        atr_safe = st.atr if st.atr and st.atr > 0 else 1e-9

        body = abs(close - open_)
        desp_fuerte = body > atr_safe * cfg.disp_multiplier

        bos_alcista = len(highs) > cfg.bos_lookback + 1 and high > max(highs[-(cfg.bos_lookback + 1):-1])
        bos_bajista = len(lows) > cfg.bos_lookback + 1 and low < min(lows[-(cfg.bos_lookback + 1):-1])

        ph, pl = self._update_pivots(st, cfg.piv_len)
        if ph is not None:
            st.max_validos += 1
            st.swing_high = ph
        if pl is not None:
            st.min_validos += 1
            st.swing_low = pl

        if bos_alcista:
            st.struct_trend = 1
        if bos_bajista:
            st.struct_trend = -1

        # ---- 2b. CHoCH [PORT EXACTO] ----
        choch_down = st.struct_trend == 1 and st.swing_low is not None and close < st.swing_low
        choch_up = st.struct_trend == -1 and st.swing_high is not None and close > st.swing_high
        choch_up_event = choch_up and not st.prev_choch_up
        choch_down_event = choch_down and not st.prev_choch_down
        st.prev_choch_up, st.prev_choch_down = choch_up, choch_down

        st.bars_choch_up = 0 if choch_up_event else st.bars_choch_up + 1
        st.bars_choch_down = 0 if choch_down_event else st.bars_choch_down + 1
        choch_up_recent = st.bars_choch_up <= cfg.choch_window
        choch_down_recent = st.bars_choch_down <= cfg.choch_window

        if bos_alcista:
            st.ultimo_alineado = alcista
        if bos_bajista:
            st.ultimo_alineado = bajista
        if choch_up_event:
            st.ultimo_alineado = alcista
        if choch_down_event:
            st.ultimo_alineado = bajista

        # ---- 3. RANGO OPERATIVO [PORT EXACTO] ----
        window = min(cfg.hh_ll_period, len(highs))
        rango_high = max(highs[-window:])
        rango_low = min(lows[-window:])
        rango_size = rango_high - rango_low
        rango_safe = rango_size if rango_size > 0.0001 else 0.0001
        pos_en_rango = min(max((close - rango_low) / rango_safe, 0.0), 1.0)

        # ---- 4. FORMULA BASE F1-F4 [PORT EXACTO] ----
        if len(highs) >= 3 and len(lows) >= 3:
            hh_est = highs[-1] > highs[-2] > highs[-3]
            hl_est = lows[-1] > lows[-2] > lows[-3]
            ll_est = lows[-1] < lows[-2] < lows[-3]
            lh_est = highs[-1] < highs[-2] < highs[-3]
        else:
            hh_est = hl_est = ll_est = lh_est = False

        st.piernas_alc = st.piernas_alc + 1 if (hh_est or hl_est) else 0
        st.piernas_baj = st.piernas_baj + 1 if (ll_est or lh_est) else 0
        piernas_activas = st.piernas_alc if alcista else st.piernas_baj

        f1 = min(piernas_activas / 5.0, 1.0)
        f2 = (1.0 - pos_en_rango) if alcista else pos_en_rango
        f3 = 1.0 if st.ultimo_alineado else 0.15
        st.velas_sin_bos = 0 if (bos_alcista or bos_bajista) else st.velas_sin_bos + 1
        f4 = max(1.0 - (st.velas_sin_bos / 20.0), 0.0)

        prob_final = round((f1 * cfg.w1 + f2 * cfg.w2 + f3 * cfg.w3 + f4 * cfg.w4) * 100)
        bos_score = 5 if prob_final >= 80 else 4 if prob_final >= 65 else 3 if prob_final >= 50 else 2 if prob_final >= 35 else 1

        # ---- 5. contUp/contDn, retest (para grado) [PORT EXACTO] ----
        f_up_legs = min(st.piernas_alc / 5.0, 1.0)
        f_dn_legs = min(st.piernas_baj / 5.0, 1.0)
        cont_up = round((f_up_legs * 0.35 + (1.0 - pos_en_rango) * 0.25 + (1.0 if (st.ultimo_alineado and alcista) else 0.3) * 0.25 + f4 * 0.15) * 100)
        cont_dn = round((f_dn_legs * 0.35 + pos_en_rango * 0.25 + (1.0 if (st.ultimo_alineado and bajista) else 0.3) * 0.25 + f4 * 0.15) * 100)
        retest_up = min(cont_up + 20, 95)
        retest_dn = min(cont_dn + 20, 95)

        # ---- 6. GESTION DE ORDER BLOCKS [PORT EXACTO + eviccion RECONSTRUIDA] ----
        bull_ob_base = bos_alcista and close1 < open1
        bear_ob_base = bos_bajista and close1 > open1
        bull_ob = (bull_ob_base and desp_fuerte) if cfg.require_disp else bull_ob_base
        bear_ob = (bear_ob_base and desp_fuerte) if cfg.require_disp else bear_ob_base
        bull_ob_ok = bull_ob and low1 >= rango_low and open1 <= rango_high
        bear_ob_ok = bear_ob and open1 >= rango_low and high1 <= rango_high

        # Invalidacion (loop reversa en Pine -- ac치 iteramos una copia y
        # reconstruimos la lista, mismo resultado)
        still_alive = []
        for ob in st.obs:
            roto = (close < ob.bottom) if ob.is_bull else (close > ob.top)
            if roto:
                if ob.touched:
                    st.mem_hold.append(0.0)
                    if len(st.mem_hold) > cfg.historical_zones:
                        st.mem_hold.pop(0)
                self.zone_cache.mark_broken_by_id(symbol, ob.zone_id)
            else:
                still_alive.append(ob)
        st.obs = still_alive

        # Creacion
        if prob_final >= cfg.min_prob_ob:
            if bull_ob_ok:
                st.obs.append(OBRecord(
                    zone_id=f"ob_{symbol}_{tf_min}_{uuid.uuid4().hex[:10]}",
                    top=open1, bottom=low1, is_bull=True, prob_final=prob_final, bos_score=bos_score,
                ))
            if bear_ob_ok:
                st.obs.append(OBRecord(
                    zone_id=f"ob_{symbol}_{tf_min}_{uuid.uuid4().hex[:10]}",
                    top=high1, bottom=open1, is_bull=False, prob_final=prob_final, bos_score=bos_score,
                ))

        # [RECONSTRUIDO -- el Pine deja esto como "while ... find min prob
        # index -> remove all arrays at idx", sin el codigo del loop.
        # Implementado literal a la descripcion: mientras haya mas OB que
        # maxOBActivos, borrar el de menor prob_final (estatico, el de
        # creacion -- es lo que Pine guarda en obProbs).]
        while len(st.obs) > cfg.max_ob_activos:
            worst = min(st.obs, key=lambda o: o.prob_final)
            st.obs.remove(worst)
            self.zone_cache.mark_broken_by_id(symbol, worst.zone_id)

        # ---- 7. FVG [PORT parcial + mitigacion/limite RECONSTRUIDOS] ----
        if cfg.fvg_enabled:
            fvg_bull = low > high2 and close1 > high2
            fvg_bear = high < low2 and close1 < low2

            # [RECONSTRUIDO] mitigacion: "precio entra en gap -> borrar" --
            # se interpreta como cualquier vela posterior cuyo low
            # (bull)/high (bear) re-entra al rango del gap.
            still_fvg = []
            for g in st.fvgs:
                mitigated = (low <= g.bottom) if g.is_bull else (high >= g.top)
                if not mitigated:
                    still_fvg.append(g)
            st.fvgs = still_fvg

            if fvg_bull:
                st.fvgs.append(FVGRecord(zone_id=f"fvg_{symbol}_{tf_min}_{uuid.uuid4().hex[:10]}", top=low, bottom=high2, is_bull=True, target=high2, created_bar=st.bar_count))
            if fvg_bear:
                st.fvgs.append(FVGRecord(zone_id=f"fvg_{symbol}_{tf_min}_{uuid.uuid4().hex[:10]}", top=low2, bottom=high, is_bull=False, target=low2, created_bar=st.bar_count))

            # [RECONSTRUIDO] limite maxFvgActivos: FIFO (se descarta el mas viejo)
            while len(st.fvgs) > cfg.max_fvg_activos:
                st.fvgs.pop(0)

        # ---- 8. APRENDIZAJE [PORT EXACTO] ----
        ob_hold_rate = (statistics.fmean(st.mem_hold) * 100.0) if st.mem_hold else 50.0
        learned_reach_atr = max(0.5, statistics.fmean(st.mem_reach_atr)) if st.mem_reach_atr else 2.0

        # ---- 9. PROXIMIDAD + TASA DINAMICA [PORT EXACTO] ----
        en_ob = en_compra = en_venta = False
        opp_dist = same_dist = -1.0
        zone_compra_bottom = zone_compra_top = None
        zone_venta_bottom = zone_venta_top = None

        for ob in st.obs:
            d_price = (ob.bottom - close) if close < ob.bottom else (close - ob.top) if close > ob.top else 0.0
            d_atr = d_price / atr_safe
            if ob.approach_d < 0 and d_atr <= learned_reach_atr * 1.5 and d_price > 0:
                ob.approach_d = d_atr
            if d_price == 0.0:
                en_ob = True
                if ob.is_bull:
                    en_compra = True
                    if zone_compra_bottom is None:
                        zone_compra_bottom, zone_compra_top = ob.bottom, ob.top
                else:
                    en_venta = True
                    if zone_venta_bottom is None:
                        zone_venta_bottom, zone_venta_top = ob.bottom, ob.top
                if not ob.touched:
                    ob.touched = True
                    ob.touch_bar = st.bar_count
                    self.zone_cache.mark_touch(symbol, ob.zone_id, now_ms=bar.ts_ms)

            if ob.touched:
                alejado = (close - ob.top > atr_safe) if ob.is_bull else (ob.bottom - close > atr_safe)
                if alejado:
                    st.mem_hold.append(1.0)
                    if ob.approach_d > 0:
                        st.mem_reach_atr.append(ob.approach_d)
                        if len(st.mem_reach_atr) > cfg.historical_zones:
                            st.mem_reach_atr.pop(0)
                    if len(st.mem_hold) > cfg.historical_zones:
                        st.mem_hold.pop(0)
                    ob.touched = False

            opposing = (not ob.is_bull) if alcista else ob.is_bull
            if opposing:
                if opp_dist < 0 or d_price < opp_dist:
                    opp_dist = d_price
            else:
                if same_dist < 0 or d_price < same_dist:
                    same_dist = d_price

        zone_reach = atr_safe * learned_reach_atr
        prox_opp = 0.0 if (opp_dist < 0 or zone_reach <= 0) else max(0.0, min(1.0, 1.0 - opp_dist / zone_reach))
        prox_same = 0.0 if (same_dist < 0 or zone_reach <= 0) else max(0.0, min(1.0, 1.0 - same_dist / zone_reach))

        tasa_cont = float(prob_final)
        if prox_opp >= prox_same and prox_opp > 0.0:
            objetivo = 100.0 - ob_hold_rate
            tasa_cont = round(prob_final * (1.0 - prox_opp) + objetivo * prox_opp)
        elif prox_same > 0.0:
            objetivo = ob_hold_rate
            tasa_cont = round(prob_final * (1.0 - prox_same) + objetivo * prox_same)
        tasa_cont = max(0.0, min(100.0, tasa_cont))

        cont_alta = tasa_cont > cfg.cont_alta_th
        cont_baja = tasa_cont < cfg.cont_baja_th

        # Calificacion / grado [PORT parcial -- mapeo A-D RECONSTRUIDO,
        # el umbral esta descrito en el prompt pero el codigo Pine exacto
        # vino como "// ... Grado logic ..."]
        same_side_touch = en_compra if alcista else en_venta
        if same_side_touch:
            st.calif_prob = round(retest_up if alcista else retest_dn)
        grado = "A" if st.calif_prob >= 80 else "B" if st.calif_prob >= 70 else "C" if st.calif_prob >= 50 else "D"

        # ---- 9b. GATILLOS DE ENTRADA [PORT EXACTO] -- no disparan senal,
        # solo quedan en metadata; SwingExpert es quien decide, no esto ----
        risk_compra = (close - zone_compra_bottom) if zone_compra_bottom is not None else None
        risk_venta = (zone_venta_top - close) if zone_venta_top is not None else None
        risk_compra_ok = risk_compra is not None and risk_compra >= cfg.min_risk_atr * atr_safe
        risk_venta_ok = risk_venta is not None and risk_venta >= cfg.min_risk_atr * atr_safe
        cont_alta_gate = prob_final > cfg.cont_alta_th
        sig_cont_compra = en_ob and alcista and en_compra and cont_alta_gate and risk_compra_ok
        sig_cont_venta = en_ob and bajista and en_venta and cont_alta_gate and risk_venta_ok
        sig_rev_compra = en_ob and en_compra and cont_baja and choch_up_recent and risk_compra_ok
        sig_rev_venta = en_ob and en_venta and cont_baja and choch_down_recent and risk_venta_ok

        # ---- Publicar en ZoneCache ----
        for ob in st.obs:
            self.zone_cache.upsert_zone(SenseiZone(
                zone_id=ob.zone_id,
                symbol=symbol,
                side="BULLISH" if ob.is_bull else "BEARISH",
                price_high=ob.top,
                price_low=ob.bottom,
                mid_price=(ob.top + ob.bottom) / 2.0,
                probability=int(tasa_cont) if (ob.touched or (ob.bottom <= close <= ob.top)) else ob.prob_final,
                quality_score=ob.bos_score,
                mitigation="PARTIAL" if ob.touched else "UNTESTED",
                timeframe_min=tf_min,
                trend="BULLISH" if alcista else "BEARISH",
                choch_active=choch_up_recent or choch_down_recent,
                choch_dir="UP" if choch_up_recent else ("DOWN" if choch_down_recent else None),
                structure_aligned=st.ultimo_alineado,
                range_pos_pct=pos_en_rango,
                legs=piernas_activas,
                hold_rate_pct=ob_hold_rate,
                learned_reach_atr=learned_reach_atr,
                ts_ms=bar.ts_ms,
                metadata={
                    "bos_score": ob.bos_score, "orig_prob": ob.prob_final, "grado": grado,
                    "sig_cont_compra": sig_cont_compra, "sig_cont_venta": sig_cont_venta,
                    "sig_rev_compra": sig_rev_compra, "sig_rev_venta": sig_rev_venta,
                },
            ))

    # ------------------------------------------------------------------
    # on_tick -- seccion 9 recalculada intrabar contra el precio en vivo,
    # SIN crear/invalidar OB (eso solo pasa on_bar_close). Firma explicita
    # (symbol, tf_min, price, atr) -- ver limitacion en el docstring del
    # modulo: no hay snapshot.tick real conectado todavia.
    # ------------------------------------------------------------------
    def on_tick(self, symbol: str, tf_min: int, price: float, now_ms: int) -> None:
        st = self.state.get((symbol, tf_min))
        if st is None or st.atr is None or not st.obs:
            return
        atr_safe = st.atr if st.atr > 0 else 1e-9
        ob_hold_rate = (statistics.fmean(st.mem_hold) * 100.0) if st.mem_hold else 50.0
        learned_reach_atr = max(0.5, statistics.fmean(st.mem_reach_atr)) if st.mem_reach_atr else 2.0

        for ob in st.obs:
            d_price = (ob.bottom - price) if price < ob.bottom else (price - ob.top) if price > ob.top else 0.0
            d_atr = d_price / atr_safe
            zone_reach = atr_safe * learned_reach_atr
            prox = 0.0 if zone_reach <= 0 else max(0.0, min(1.0, 1.0 - d_atr))
            if d_price == 0.0 or prox <= 0.0:
                continue
            objetivo = 100.0 - ob_hold_rate if prox > 0.5 else ob_hold_rate  # aproximacion intrabar liviana, ver nota
            tasa_cont = max(0.0, min(100.0, round(ob.prob_final * (1.0 - prox) + objetivo * prox)))
            self.zone_cache.upsert_zone(SenseiZone(
                zone_id=ob.zone_id, symbol=symbol, side="BULLISH" if ob.is_bull else "BEARISH",
                price_high=ob.top, price_low=ob.bottom, mid_price=(ob.top + ob.bottom) / 2.0,
                probability=int(tasa_cont), quality_score=ob.bos_score,
                mitigation="PARTIAL" if ob.touched else "UNTESTED", timeframe_min=tf_min,
                trend="BULLISH" if ob.is_bull else "BEARISH", range_pos_pct=0.5, legs=0,
                hold_rate_pct=ob_hold_rate, learned_reach_atr=learned_reach_atr, ts_ms=now_ms,
            ))

    # ------------------------------------------------------------------
    # Indicadores incrementales
    # ------------------------------------------------------------------
    @staticmethod
    def _update_ema(prev_ema: Optional[float], close: float, period: int) -> float:
        if prev_ema is None:
            return close
        k = 2.0 / (period + 1)
        return prev_ema + k * (close - prev_ema)

    @staticmethod
    def _update_atr(st: SenseiState, tr: float, period: int) -> float:
        if st.atr is None:
            st._atr_seed_trs.append(tr)
            if len(st._atr_seed_trs) < period:
                return statistics.fmean(st._atr_seed_trs)
            return statistics.fmean(st._atr_seed_trs)
        # Wilder's smoothing, igual que ta.atr() de Pine
        return (st.atr * (period - 1) + tr) / period

    @staticmethod
    def _update_pivots(st: SenseiState, piv_len: int) -> tuple[Optional[float], Optional[float]]:
        """ta.pivothigh/low(src, pivLen, pivLen): el bar candidato es el que
        esta pivLen barras atras del cierre actual (necesita pivLen barras
        de cada lado para confirmarse) -- por eso el pivote 'aparece' con
        pivLen barras de delay, igual que en Pine."""
        window = 2 * piv_len + 1
        if len(st.highs) < window:
            return None, None
        highs_w = list(st.highs)[-window:]
        lows_w = list(st.lows)[-window:]
        center = piv_len
        ph = highs_w[center] if highs_w[center] == max(highs_w) else None
        pl = lows_w[center] if lows_w[center] == min(lows_w) else None
        return ph, pl
