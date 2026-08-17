"""MarketSnapshot: puente entre src/snapshot_engine.SnapshotEngine (la unica
fuente de datos real que existe hoy) y los dos expertos.

No existe una clase MarketSnapshot en el codigo real -- SnapshotEngine.get_live_snapshot()
devuelve un dict plano pensado para src/order_flow_signal.decide(). El cerebro swing
(HTFFundingBrain.decide) espera un "vela" y un "htf" con OTRA forma (ver comentarios
abajo). Esta clase construye ambas vistas a partir del MISMO estado interno del
SnapshotEngine, sin duplicar ni reinterpretar la logica de deteccion de patrones.

HTFFundingBrain espera htf["atr14_15m"] y htf["vwap_15m"] en PRECIO directo.
SnapshotEngine los calcula ahora de verdad (_recompute_atr14_15m /
_recompute_vwap_15m, sobre velas M15 propias) -- ver src/snapshot_engine.py.
Quedan en None hasta juntar suficiente historia (15 * (ATR14_15M_PERIODS+1)
velas 1m, ~3.75h desde que arranca el engine); HTFFundingBrain.decide() ya
trata atr14_15m=None como "ATR invalido" y no dispara, asi que no hace falta
un fallback inventado aca.

BUG CORREGIDO (P0 -> P1): la primera version de este puente usaba
htf_cache["atr_30m"] como proxy de atr14_15m. atr_30m esta expresado en
TICKS a proposito (ver _recompute_atr_30m, es lo que necesita
order_flow_signal.decide()), no en precio -- pasarlo directo a
HTFFundingBrain (que hace `entry ± atr * sl_atr_mult` en precio sin
reconvertir) daba distancias de SL ~1/TICK_SIZE veces mas anchas de lo
debido. Con atr14_15m real esto ya no aplica.
"""
from __future__ import annotations

from typing import Any, Literal, Optional

from pydantic import BaseModel, Field


class BarData(BaseModel):
    """Vela OHLCV cerrada de un timeframe agregado (15m/1h/4h/1D), producida
    por SnapshotEngine desde sus propias velas 1m -- ver
    SnapshotEngine._cerrar_vela_actual()/_aggregate_to_m30/_aggregate_to_h4/
    _aggregate_to_1h/_aggregate_to_1d. ts_ms (no timestamp_ms) para ser
    consistente con MarketSnapshot.ts_ms -- una sola unidad/nombre de tiempo
    en todo el paquete schemas/."""
    ts_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    delta: Optional[float] = Field(
        default=None,
        description="buy_vol - sell_vol de la vela (taker_buy_base_vol - taker_sell_vol en "
        "klines historicos, ver orderflow_data.load_klines). Opcional/aditivo -- None para "
        "quien construya un BarData sin este dato (SenseiZoneGenerator nunca lo lee); lo "
        "consume OrderFlowEngine.on_bar_close(), que SI lo requiere.",
    )


class MarketSnapshot(BaseModel):
    ts_ms: int
    symbol: str
    equity_usdt: float
    open_positions: list[dict[str, Any]] = Field(default_factory=list)

    scalping_payload: dict[str, Any] = Field(
        description="Tal cual lo espera src/order_flow_signal.decide(payload, equity, risk_manager) "
        "como primer argumento -- es exactamente el dict de SnapshotEngine.get_live_snapshot()."
    )
    htf_vela: dict[str, Any] = Field(
        description="Tal cual lo espera HTFFundingBrain.decide(vela, htf, ts_ms) como primer "
        "argumento: close/high/low/delta/volume + abs/div/stop_run/initiative_pullback/breakout_vol."
    )
    htf_context: dict[str, Any] = Field(
        description="Tal cual lo espera HTFFundingBrain.decide(vela, htf, ts_ms) como segundo "
        "argumento: poc/vah/val/swing_h4_h/swing_h4_l/vwap_15m/cvd_trend/atr14_15m."
    )

    # Opcional -- None hasta que OrderFlowEngine este conectado al pipeline
    # en vivo (todavia no lo esta). ScalpingExpert.analyze() no lo consulta
    # todavia -- mismo criterio que zone_cache/Sensei: no se activa como
    # filtro de confluencia hasta un A/B real contra el backtest ya validado.
    orderflow: Optional[OrderFlowSnapshot] = None

    # --- Multi-TF para SenseiZoneGenerator (opcional -- nada rompe si quedan
    # vacios; ScalpingExpert/SwingExpert no los leen, solo lo hara
    # SenseiZoneGenerator.on_bar_close cuando SignalOrchestrator lo llame) ---
    bars_15m: list[BarData] = Field(default_factory=list, description="Ultimas velas 15m cerradas (cola, la [-1] es la mas reciente).")
    bars_1h: list[BarData] = Field(default_factory=list)
    bars_4h: list[BarData] = Field(default_factory=list)
    bars_1d: list[BarData] = Field(default_factory=list)
    closed_timeframes: list[int] = Field(default_factory=list, description="Que TFs (en minutos: 15/60/240/1440) cerraron vela en ESTE tick -- vacio la gran mayoria de los ticks.")

    def is_bar_closed(self, tf_min: int) -> bool:
        return tf_min in self.closed_timeframes

    def get_last_closed_bar(self, tf_min: int) -> BarData | None:
        bars = {15: self.bars_15m, 60: self.bars_1h, 240: self.bars_4h, 1440: self.bars_1d}.get(tf_min)
        return bars[-1] if bars else None

    @classmethod
    def from_snapshot_engine(cls, engine: Any, equity_usdt: float, open_positions: list[dict] | None = None) -> "MarketSnapshot":
        """engine: instancia real de src.snapshot_engine.SnapshotEngine."""
        open_positions = open_positions or []
        scalping_payload = engine.get_live_snapshot(equity_usdt=equity_usdt, open_positions=open_positions)

        klines = list(engine.klines_1m)
        ultima = klines[-1] if klines else {}
        htf_vela = {
            "close": ultima.get("c"),
            "high": ultima.get("_high"),
            "low": ultima.get("_low"),
            "delta": ultima.get("delta"),
            "volume": ultima.get("vol"),
            "abs": ultima.get("abs", False),
            "div": ultima.get("div", False),
            "stop_run": ultima.get("stop_run"),
            "initiative_pullback": ultima.get("initiative_pullback"),
            "breakout_vol": ultima.get("breakout_vol"),
        }

        htf_cache = engine.htf  # property publica -> self._htf_cache
        htf_context = {
            "poc": htf_cache.get("poc"),
            "vah": htf_cache.get("vah"),
            "val": htf_cache.get("val"),
            "cvd_trend": htf_cache.get("cvd_trend"),
            "swing_h4_h": htf_cache.get("swing_high_h4"),
            "swing_h4_l": htf_cache.get("swing_low_h4"),
            "atr14_15m": htf_cache.get("atr14_15m"),
            "vwap_15m": htf_cache.get("vwap_15m"),
            "atr_rank_30d": htf_cache.get("atr_rank_30d"),
        }

        ts_ms = ultima.get("_open_time")
        ts_ms = int(ts_ms * 1000) if ts_ms is not None else 0

        # Multi-TF para SenseiZoneGenerator -- ver SnapshotEngine.bars_multi_tf()/
        # closed_timeframes_this_close (metodos nuevos, aditivos). getattr con
        # default: si engine es un mock/version vieja sin estos metodos (tests
        # existentes, por ejemplo), MarketSnapshot sigue funcionando igual que
        # antes, solo con estas 5 listas vacias.
        def _to_bardata(c: dict) -> BarData:
            return BarData(ts_ms=int(c["open_time"] * 1000), open=c["open"], high=c["high"], low=c["low"], close=c["close"], volume=c["volume"])

        bars_multi_tf = engine.bars_multi_tf() if hasattr(engine, "bars_multi_tf") else {}
        closed_tfs = engine.closed_timeframes_this_close() if hasattr(engine, "closed_timeframes_this_close") else []

        return cls(
            ts_ms=ts_ms,
            symbol=engine.symbol.upper() if hasattr(engine, "symbol") else "BTCUSDT",
            equity_usdt=equity_usdt,
            open_positions=open_positions,
            scalping_payload=scalping_payload,
            htf_vela=htf_vela,
            htf_context=htf_context,
            bars_15m=[_to_bardata(c) for c in bars_multi_tf.get(15, [])],
            bars_1h=[_to_bardata(c) for c in bars_multi_tf.get(60, [])],
            bars_4h=[_to_bardata(c) for c in bars_multi_tf.get(240, [])],
            bars_1d=[_to_bardata(c) for c in bars_multi_tf.get(1440, [])],
            closed_timeframes=closed_tfs,
        )


# ----------------------------------------------------------------------
# OrderFlowEngine (Fase 1) -- salida de src/services/orderflow_engine.py.
#
# TRANSPARENCIA SOBRE QUE ES REAL Y QUE ES APROXIMADO (mismo criterio que
# orderflow_data.py, ver docstring de ese modulo): el dataset disponible
# (config/paths.yaml -> btcusd_order_flow) son klines de Binance -- 12
# columnas agregadas por vela (M1/M5/15m/1h), NO trades tick-a-tick ni
# profundidad L2 del book. Eso limita lo que se puede calcular de verdad:
#
# - CVD / delta: REALES -- se derivan de taker_buy_base_vol vs el volumen
#   vendedor agresivo implicito (volume - taker_buy_base_vol), igual que
#   orderflow_data.load_klines() ya hace para el resto del pipeline.
# - Footprint (POC/VAH/VAL/VWAP): APROXIMADO -- volume profile sobre una
#   ventana rolling de velas (ofd.volume_profile), no por nivel de precio
#   dentro de una sola vela (eso requeriria tick data que no existe aca).
# - Absorption / Exhaustion / Delta Trap: REALES en el sentido de que se
#   derivan de OHLCV+delta real por vela, pero la deteccion en si es una
#   heuristica (percentil de volumen, divergencia precio/delta, reclaim de
#   high/low) -- no observan el tape trade a trade.
# - Stack Imbalance: NO CALCULABLE con este dataset (requiere trades por
#   nivel de precio o profundidad L2, igual que las imbalances 3:1 de
#   SnapshotEngine._evaluar_iceberg/_cerrar_vela_actual en vivo). Se
#   devuelve None explicito en vez de inventar un valor -- mismo criterio
#   que orderflow_data.empty_bookmap().
# ----------------------------------------------------------------------


class FootprintData(BaseModel):
    """Volume profile aproximado sobre una ventana rolling de velas
    cerradas (ver OrderFlowEngineConfig.footprint.lookback_bars) -- NO es
    un footprint intrabar real (eso necesitaria tick data, ver nota arriba
    del modulo). poc/vah/val vienen de ofd.volume_profile(); vwap es
    sum(close*volume)/sum(volume) de la misma ventana."""
    ts_ms: int
    tf_min: int
    poc: float
    vah: float
    val: float
    vwap: float
    lookback_bars: int


class AbsorptionZone(BaseModel):
    """Vela donde volumen alto (percentil de la ventana reciente) no logro
    mover el precio (rango chico) con delta mayormente en contra del
    cierre -- ver ofd.evaluar_absorcion(), formula real reusada tal cual."""
    ts_ms: int
    tf_min: int
    price: float
    side: Literal["LONG", "SHORT"] = Field(description="LONG = absorcion de venta (compradores absorbiendo oferta), SHORT = absorcion de compra")
    volume: float
    delta: float
    volume_usd: float


class ExhaustionSignal(BaseModel):
    """Divergencia precio/delta + decaimiento de volumen sobre las ultimas
    N velas -- ver ofd.evaluar_exhaustion(). direction es hacia donde
    apunta el agotamiento (LONG = agotamiento de vendedores, posible giro
    alcista; SHORT = agotamiento de compradores)."""
    ts_ms: int
    tf_min: int
    price: float
    direction: Literal["LONG", "SHORT"]
    delta_divergence_pct: float
    volume_decay_pct: float


class DeltaTrap(BaseModel):
    """Failed breakout: la vela barre (wick) un high/low previo pero
    cierra de vuelta adentro (reclaim) con delta a favor de la reversion --
    ver ofd.evaluar_stop_run(), formula real reusada tal cual (mismo
    concepto que "stop run" en SnapshotEngine, aca expuesto como DeltaTrap
    para el vocabulario de OrderFlowEngine)."""
    ts_ms: int
    tf_min: int
    price: float
    direction: Literal["LONG", "SHORT"]
    price_excursion_ticks: float


class StackImbalance(BaseModel):
    """Placeholder de forma -- OrderFlowEngine.get_stack_imbalance() SIEMPRE
    devuelve None con este dataset (klines agregados, sin trades por nivel
    de precio ni L2). El tipo queda definido para cuando haya un feed real
    (SnapshotEngine en vivo SI tiene lo necesario, ver
    SnapshotEngine._evaluar_iceberg), no se instancia hoy."""
    ts_ms: int
    tf_min: int
    price: float
    side: Literal["bid", "ask"]
    ratio: float
    volume_usd: float


class OrderFlowSnapshot(BaseModel):
    """Salida de OrderFlowEngine.snapshot() -- todo el estado de microestructura
    de un simbolo en un instante dado, para sumarse a MarketSnapshot."""
    ts_ms: int
    symbol: str
    cvd: float
    delta_1m: float = 0.0
    delta_5m: float = 0.0
    footprint_1m: Optional[FootprintData] = None
    absorption_zones: list[AbsorptionZone] = Field(default_factory=list)
    exhaustion_signals: list[ExhaustionSignal] = Field(default_factory=list)
    delta_traps: list[DeltaTrap] = Field(default_factory=list)
    stack_imbalance: Optional[StackImbalance] = Field(
        default=None,
        description="Siempre None con el dataset de klines agregados actual -- ver nota de "
        "transparencia arriba del modulo. No se inventa un valor.",
    )


# MarketSnapshot.orderflow referencia OrderFlowSnapshot antes de que esta
# clase exista en el modulo (forward ref via `from __future__ import
# annotations`) -- pydantic v2 no la resuelve sola al primer uso, hay que
# pedirselo explicitamente una vez que ambas clases ya estan definidas.
MarketSnapshot.model_rebuild()
