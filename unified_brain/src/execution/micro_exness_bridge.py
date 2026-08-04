"""Wrapper de cuenta chica ($100-$300) para Exness Raw/Zero, sobre ETHUSDT.

USA EL MOTOR REAL: src.order_flow_signal.decide(payload, equity, risk_manager)
-- firma real confirmada (grep en el modulo, no existe OrderFlowSignalEngine
ni clases Signal/SignalType, esa API no existe en este repo).

QUE SI HACE este archivo: sesion horaria, limite de trades/dia, guarda de
perdida diaria, guarda de costo-vs-riesgo, sizing en lotes ETH, construccion
de SL/TP calibrados para la escala de precio de ETH (no reusa los
$250/$500/$750 de config/constants.py, esos son para BTC ~$65k -- ver
scripts/calibrate_eth_params.py).

QUE NO HACE (y por que no esta ac�): decide() necesita contexto HTF real
(poc/vah/val/swing_high_h4/swing_low_h4/cvd_trend) y flags de deteccion por
vela (div/abs/stop_run/initiative_pullback/breakout_vol) -- eso sale de un
pipeline de precomputo real (calcular_htf/calcular_features_1m/
agregar_initiative_pullback en backtest_standard_account.py), no de un
buffer de velas OHLCV crudas. Reimplementar ese pipeline en este archivo
seria duplicar logica de deteccion ya real y validada para BTC -- en cambio,
este bridge ESPERA recibir la vela ya enriquecida con esos campos (el mismo
shape que "fila" en el loop de backtest_standard_account.py). Para
backtestear ETH, el caller debe correr esas mismas funciones reales sobre el
DataFrame de ETH antes de iterar bar-by-bar ac�, igual que ya se hace con
BTC -- no se inventa un pipeline nuevo.

Payload real que arma este archivo para decide() (confirmado leyendo
backtest_standard_account.py:672-680):
    htf = {"poc","vah","val","swing_high_h4","swing_low_h4","cvd_trend"}
    ltf_1m = [{"t","c","delta","vol","imb","iceberg","div","abs",
                "stop_run","initiative_pullback","breakout_vol","liquidity_zone"}, ...]
    payload = {"htf_ready": True, "snapshot": {"htf": htf, "ltf_1m": ltf_1m}}

Diseno deliberado: SL/TP de decide() (senal["stop_loss_price"]/senal["tp_levels"])
se DESCARTAN -- estan calibrados con SL_PIPS/TP1_PIPS/TP2_PIPS de
config/constants.py (dolares fijos para BTC). Se usa decide() solo para la
señal de entrada (direccion/conviction/setup_type); SL/TP/sizing se
recalculan ac� con los parametros calibrados de ETH (MicroParams.SL_USD/
TP1_USD/TP2_USD, ver scripts/calibrate_eth_params.py). Evita monkeypatchear
constantes globales (no es thread/modulo-safe) y evita usar un bracket
calibrado para un activo de otra escala de precio.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

from src.order_flow_signal import decide


@dataclass
class MicroParams:
    equity_usd: float
    symbol: str = "ETHUSDT"
    risk_pct: float = 0.01            # 1% de riesgo/trade
    max_daily_dd_pct: float = 0.03    # 3% stop duro diario
    max_trades_day: int = 3           # limite duro -- evita sangrado de comision por overtrading
    min_conviction: float = 0.65
    commission_rt_001lot: float = 0.07  # USD, ida+vuelta a 0.01 lote (ver docstring de scripts/download_eth_data.py)
    cost_guard_max_pct_of_risk: float = 0.15  # si comision > 15% del riesgo en $, se salta el trade
    session_start_utc: int = 14
    session_end_utc: int = 17         # HARD FLAT a esta hora
    warmup_minutes: int = 15
    min_lot: float = 0.01
    lot_step: float = 0.01
    # CALIBRADOS -- placeholders, reemplazar con la salida real de
    # scripts/calibrate_eth_params.py antes de operar (no son datos, son
    # supuestos de diseño hasta que ese script corra contra datos reales).
    SL_USD: float = 45.0
    TP1_USD: float = 112.5
    TP2_USD: float = 180.0


class MicroExnessBridge:
    def __init__(self, params: MicroParams, risk_mgr: Any) -> None:
        """risk_mgr: instancia real de src.risk_manager.RiskManager (ej.
        RiskManager(None, capital_inicial=params.equity_usd)) -- se le pasa
        a decide() tal cual, que lo usa internamente para su propio sizing
        (que este bridge ignora, ver docstring del modulo)."""
        self.p = params
        self.risk = risk_mgr
        self.bars_buffer: list[dict] = []
        self.daily_pnl: float = 0.0
        self.trades_today: int = 0
        self.trading_allowed: bool = True
        self.position: Optional[dict] = None
        self._session_start_ts: Optional[Any] = None

    # ------------------------------------------------------------------
    def on_bar_closed(self, bar: dict) -> Optional[dict]:
        """bar: vela YA ENRIQUECIDA (ver docstring del modulo) con al menos:
        timestamp (UTC, pd.Timestamp), open/high/low/close/volume,
        taker_buy_base_vol, poc, vah, val, swing_high_h4, swing_low_h4,
        cvd_trend, y opcionalmente div/abs/stop_run/initiative_pullback/
        breakout_vol (default False/"" si no vienen)."""
        ts = bar["timestamp"]
        hour = ts.hour

        if hour >= self.p.session_end_utc and self.position is not None:
            return self._flat("EOD_HARD_FLAT")

        in_session = self.p.session_start_utc <= hour < self.p.session_end_utc
        if not in_session:
            return None
        if hour == self.p.session_start_utc and ts.minute < self.p.warmup_minutes:
            return None

        if not self.trading_allowed:
            return None

        dd_limit_usd = self.p.equity_usd * self.p.max_daily_dd_pct
        if self.daily_pnl <= -dd_limit_usd:
            self.trading_allowed = False
            if self.position is not None:
                return self._flat("DAILY_DD_GUARD")
            return None

        self._append_bar(bar)

        if self.position is not None:
            return None  # v1: sin trailing, ver _manage_trailing (stub)

        if self.trades_today >= self.p.max_trades_day:
            return None

        payload = self._build_payload()
        senal = decide(payload, self.p.equity_usd, self.risk)

        if senal["decision"] not in ("LONG", "SHORT"):
            return None
        if senal.get("conviction", 0) / 10.0 < self.p.min_conviction:
            return None

        return self._build_order(senal, bar)

    def on_fill(self, fill_price: float, fill_qty: float, tag: str) -> None:
        if self.position is None:
            return
        is_close = any(k in tag for k in ("TP", "SL", "FLAT"))
        if not is_close:
            return
        side_sign = 1 if self.position["side"] == "BUY" else -1
        pnl_gross = (fill_price - self.position["entry"]) * fill_qty * side_sign
        commission = self.p.commission_rt_001lot * (fill_qty / self.p.min_lot)
        self.daily_pnl += pnl_gross - commission
        # Cierre parcial (ej. TP1 con 50% del size): reduce el remanente en
        # vez de limpiar la posicion -- si no, el siguiente on_bar_closed
        # dejaria abrir un trade nuevo mientras el runner de TP2/SL sigue
        # vivo. Solo se limpia cuando el remanente llega a ~0.
        self.position["size"] -= fill_qty
        if self.position["size"] <= self.p.min_lot / 2:
            self.position = None

    def daily_reset(self, new_equity: float) -> None:
        self.p.equity_usd = new_equity
        self.daily_pnl = 0.0
        self.trades_today = 0
        self.trading_allowed = True
        self.position = None
        self.bars_buffer.clear()

    # ------------------------------------------------------------------
    def _append_bar(self, bar: dict) -> None:
        volume = bar["volume"]
        taker_buy = bar["taker_buy_base_vol"]
        delta = taker_buy - (volume - taker_buy)  # mismo criterio que src/orderflow_data.py
        self.bars_buffer.append({
            "t": bar["timestamp"], "c": bar["close"], "delta": delta, "vol": volume,
            "imb": [], "iceberg": 0,
            "div": bool(bar.get("div", False)), "abs": bool(bar.get("abs", False)),
            "stop_run": bar.get("stop_run") or "", "initiative_pullback": bar.get("initiative_pullback") or "",
            "breakout_vol": bar.get("breakout_vol") or "", "liquidity_zone": None,
            "_htf": {
                "poc": bar.get("poc"), "vah": bar.get("vah"), "val": bar.get("val"),
                "swing_high_h4": bar.get("swing_high_h4"), "swing_low_h4": bar.get("swing_low_h4"),
                "cvd_trend": bar.get("cvd_trend"),
            },
        })
        if len(self.bars_buffer) > 200:
            self.bars_buffer.pop(0)

    def _build_payload(self) -> dict:
        htf = self.bars_buffer[-1]["_htf"]
        ltf_1m = [{k: v for k, v in b.items() if k != "_htf"} for b in self.bars_buffer[-100:]]
        return {"htf_ready": True, "snapshot": {"htf": htf, "ltf_1m": ltf_1m}}

    def _build_order(self, senal: dict, bar: dict) -> Optional[dict]:
        is_long = senal["decision"] == "LONG"
        side = "BUY" if is_long else "SELL"
        entry = bar["close"]
        sl = entry - self.p.SL_USD if is_long else entry + self.p.SL_USD

        risk_usd = self.p.equity_usd * self.p.risk_pct
        risk_per_lot = abs(entry - sl)
        if risk_per_lot == 0:
            return None
        raw_lots = risk_usd / risk_per_lot
        lots = max(self.p.min_lot, round(raw_lots / self.p.lot_step) * self.p.lot_step)

        # BUG REAL corregido: el guard comparaba la comision de UN lote minimo
        # (0.01) contra el riesgo, no la comision del lote REALMENTE calculado
        # -- con equity chica y SL ajustado, raw_lots suele superar 0.01 varias
        # veces (ej. $2 riesgo / $4.88 SL = 0.41 lotes), y la comision escala
        # con el tamaño real, no con el minimo. El guard nunca rechazaba nada
        # relevante porque comparaba contra el numero equivocado (comision de
        # 0.01 lote, ~$0.07, vs. la real de 0.41 lotes, ~$2.87 -- mas grande
        # que todo el riesgo del trade). Confirmado viendo r_multiple=-2.43 en
        # SLs que deberian dar exactamente -1.0R.
        commission_estimada = self.p.commission_rt_001lot * (lots / self.p.min_lot)
        if commission_estimada > risk_usd * self.p.cost_guard_max_pct_of_risk:
            return None  # cuenta demasiado chica para este riesgo -- comision se come el edge

        tp1 = entry + self.p.TP1_USD if is_long else entry - self.p.TP1_USD
        tp2 = entry + self.p.TP2_USD if is_long else entry - self.p.TP2_USD

        self.trades_today += 1
        order = {
            "action": "OPEN", "side": side, "size": lots,
            "entry": round(entry, 2), "sl": round(sl, 2), "tp1": round(tp1, 2), "tp2": round(tp2, 2),
            "tag": f"MICRO_{senal['setup_type']}_{self.trades_today}",
        }
        self.position = {"side": side, "entry": entry, "size": lots}
        return order

    def _manage_trailing(self, bar: dict) -> Optional[dict]:
        """v1: sin trailing -- placeholder para EMA9/VWAP, igual criterio
        que el resto del motor scalping (no se inventa logica nueva aca)."""
        return None

    def _flat(self, reason: str) -> dict:
        pos = self.position
        self.position = None
        return {
            "action": "FLAT", "side": "SELL" if pos["side"] == "BUY" else "BUY",
            "size": pos["size"], "reason": reason, "tag": f"FLAT_{reason}",
        }
