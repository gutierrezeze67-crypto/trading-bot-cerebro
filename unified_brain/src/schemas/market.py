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

from typing import Any

from pydantic import BaseModel, Field


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
        }

        ts_ms = ultima.get("_open_time")
        ts_ms = int(ts_ms * 1000) if ts_ms is not None else 0

        return cls(
            ts_ms=ts_ms,
            symbol=engine.symbol.upper() if hasattr(engine, "symbol") else "BTCUSDT",
            equity_usdt=equity_usdt,
            open_positions=open_positions,
            scalping_payload=scalping_payload,
            htf_vela=htf_vela,
            htf_context=htf_context,
        )
