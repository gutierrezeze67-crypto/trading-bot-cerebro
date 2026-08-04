"""OrderFlowEngine (Fase 1) -- microestructura M1/M5 (CVD, Delta, Footprint,
Absorption, Exhaustion, Delta Trap) sobre klines historicos de Binance.

TRANSPARENCIA SOBRE QUE ES REAL Y QUE ES APROXIMADO: ver la nota completa en
src/schemas/market.py (seccion "OrderFlowEngine (Fase 1)"). En resumen -- el
dataset disponible (config/paths.yaml::btcusd_order_flow) son klines
agregados por vela, NO trades tick-a-tick ni L2. Por eso este motor:

- Procesa BARRAS cerradas (on_bar_close(bar), bar.delta ya viene calculado
  por orderflow_data.load_klines() desde taker_buy_base_vol), NO trades
  individuales -- no existe on_trade() como pedia el prompt original de
  Fase 1, porque no hay tick data de donde alimentarlo.
- Reusa las formulas REALES ya validadas en produccion (SnapshotEngine via
  orderflow_data.py) en vez de reimplementarlas: evaluar_absorcion(),
  evaluar_stop_run() (expuesto aca como "Delta Trap"), volume_profile()
  para el footprint. evaluar_exhaustion() es la unica formula nueva de esta
  fase (no existia antes en orderflow_data.py).
- get_stack_imbalance() SIEMPRE devuelve None -- no calculable sin trades
  por nivel de precio / L2 (ver StackImbalance en schemas/market.py).

TF-agnostico a proposito: el engine no asume que le van a dar velas 1m --
procesa lo que le pase on_bar_close() (tf_min se fija en la config, para
etiquetar la salida y para que get_delta(tf_min) sepa agregar barras base
en multiplos de tf_min). Los scripts que lo instancian (tests, backtest de
confluencia) deciden que granularidad alimentarle.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Deque, Optional

import numpy as np

from src import orderflow_data as ofd
from src.schemas.market import (
    AbsorptionZone,
    BarData,
    DeltaTrap,
    ExhaustionSignal,
    FootprintData,
    OrderFlowSnapshot,
    StackImbalance,
)

MAX_BAR_BUFFER = 500
MAX_EVENTS_BUFFER = 300


@dataclass
class OrderFlowConfig:
    tf_min: int = 1

    absorption_enabled: bool = True
    absorption_percentile_threshold: float = 95.0
    absorption_flip_ratio_threshold: float = 0.35
    absorption_lookback_bars: int = 120
    absorption_min_volume_usd: float = 500_000.0

    exhaustion_enabled: bool = True
    exhaustion_delta_divergence_pct: float = 0.7
    exhaustion_volume_decay_pct: float = 0.5
    exhaustion_lookback_bars: int = 15

    footprint_lookback_bars: int = 30
    footprint_bins: int = 50
    footprint_va_pct: float = 0.7
    tick_size: float = 0.1

    cvd_reset_session: bool = True

    delta_trap_enabled: bool = True
    delta_trap_lookback_bars: int = 20
    delta_trap_min_bars_required: int = 10

    @classmethod
    def from_yaml_dict(cls, raw: dict, tf_min: Optional[int] = None) -> "OrderFlowConfig":
        """raw: el dict ya cargado de config/orderflow.yaml (top-level, con
        las claves absorption/exhaustion/footprint/cvd/delta_trap).
        tf_min: override explicito (ver docstring del modulo) -- si es None,
        usa footprint.tf_min del yaml (default 1)."""
        absorption = raw.get("absorption") or {}
        exhaustion = raw.get("exhaustion") or {}
        footprint = raw.get("footprint") or {}
        cvd = raw.get("cvd") or {}
        delta_trap = raw.get("delta_trap") or {}
        return cls(
            tf_min=tf_min if tf_min is not None else footprint.get("tf_min", 1),
            absorption_enabled=absorption.get("enabled", True),
            absorption_percentile_threshold=absorption.get("percentile_threshold", 95.0),
            absorption_flip_ratio_threshold=absorption.get("flip_ratio_threshold", 0.35),
            absorption_lookback_bars=absorption.get("lookback_bars", 120),
            absorption_min_volume_usd=absorption.get("min_absorption_volume_usd", 500_000.0),
            exhaustion_enabled=exhaustion.get("enabled", True),
            exhaustion_delta_divergence_pct=exhaustion.get("delta_divergence_pct", 0.7),
            exhaustion_volume_decay_pct=exhaustion.get("volume_decay_pct", 0.5),
            exhaustion_lookback_bars=exhaustion.get("lookback_bars", 15),
            footprint_lookback_bars=footprint.get("lookback_bars", 30),
            footprint_bins=footprint.get("bins", 50),
            footprint_va_pct=footprint.get("va_pct", 0.7),
            tick_size=footprint.get("tick_size", 0.1),
            cvd_reset_session=cvd.get("reset_session", True),
            delta_trap_enabled=delta_trap.get("enabled", True),
            delta_trap_lookback_bars=delta_trap.get("lookback_bars", 20),
            delta_trap_min_bars_required=delta_trap.get("min_bars_required", 10),
        )


class OrderFlowEngine:
    def __init__(self, config: dict, symbol: str = "BTCUSDT", tf_min: Optional[int] = None) -> None:
        self.cfg = OrderFlowConfig.from_yaml_dict(config, tf_min=tf_min)
        self.symbol = symbol

        self._bars: Deque[BarData] = deque(maxlen=MAX_BAR_BUFFER)
        self._cvd: float = 0.0
        self._cvd_day: Optional[int] = None

        self._absorption_zones: Deque[AbsorptionZone] = deque(maxlen=MAX_EVENTS_BUFFER)
        self._exhaustion_signals: Deque[ExhaustionSignal] = deque(maxlen=MAX_EVENTS_BUFFER)
        self._delta_traps: Deque[DeltaTrap] = deque(maxlen=MAX_EVENTS_BUFFER)
        self._last_footprint: Optional[FootprintData] = None

        # Buffers dedicados del footprint -- arrays numpy directos, NO un
        # DataFrame por barra (ver ofd._volume_profile_core: perfilado real,
        # 92 barras/s con DataFrame vs miles/s con arrays numpy).
        n = self.cfg.footprint_lookback_bars
        self._fp_lows: Deque[float] = deque(maxlen=n)
        self._fp_highs: Deque[float] = deque(maxlen=n)
        self._fp_closes: Deque[float] = deque(maxlen=n)
        self._fp_vols: Deque[float] = deque(maxlen=n)

    # ------------------------------------------------------------------
    def on_bar_close(self, bar: BarData) -> None:
        """Procesa una vela cerrada. bar.delta es OBLIGATORIO aca (a
        diferencia de SenseiZoneGenerator, que nunca lo lee) -- ver
        orderflow_data.load_klines(), que ya lo calcula desde
        taker_buy_base_vol para cualquier CSV de klines de Binance."""
        if bar.delta is None:
            raise ValueError(
                "OrderFlowEngine.on_bar_close requiere bar.delta (ver "
                "orderflow_data.load_klines/resample_klines, que lo calculan "
                "desde taker_buy_base_vol -- no se puede aproximar sin eso)."
            )

        # Deteccion ANTES de appendear `bar` a self._bars (excepto exhaustion/
        # footprint, que necesitan a `bar` como el ultimo punto de su propia
        # ventana) -- mismo orden que SnapshotEngine._cerrar_vela_actual():
        # las señales de una vela se evaluan contra la historia YA CONOCIDA,
        # nunca contra si misma incluida en su propio percentil/ventana.
        previas = list(self._bars)

        self._maybe_detect_absorption(bar, previas)
        self._maybe_detect_exhaustion(bar, previas)
        self._maybe_detect_delta_trap(bar, previas)

        self._bars.append(bar)
        self._update_cvd(bar)
        self._update_footprint(bar, previas)

    def reset(self) -> None:
        self._bars.clear()
        self._cvd = 0.0
        self._cvd_day = None
        self._absorption_zones.clear()
        self._exhaustion_signals.clear()
        self._delta_traps.clear()
        self._last_footprint = None
        self._fp_lows.clear()
        self._fp_highs.clear()
        self._fp_closes.clear()
        self._fp_vols.clear()

    # ------------------------------------------------------------------
    # CVD / Delta
    # ------------------------------------------------------------------
    def _update_cvd(self, bar: BarData) -> None:
        if self.cfg.cvd_reset_session:
            day = bar.ts_ms // 86_400_000
            if self._cvd_day is not None and day != self._cvd_day:
                self._cvd = 0.0
            self._cvd_day = day
        self._cvd += bar.delta

    def get_cvd(self) -> float:
        return self._cvd

    def get_delta(self, tf_min: int) -> float:
        """Delta acumulado de las ultimas `tf_min / self.cfg.tf_min` barras
        base. El delta es aditivo bajo agregacion (delta_i = 2*taker_buy_i -
        volume_i, sumable termino a termino -- ver orderflow_data.
        resample_klines, que recalcula delta desde volumenes agregados y da
        el mismo resultado que sumar los deltas por barra), asi que esto es
        EXACTO, no una aproximacion."""
        if tf_min <= 0 or tf_min % self.cfg.tf_min != 0:
            raise ValueError(
                f"tf_min={tf_min} debe ser multiplo positivo del tf_min base del engine ({self.cfg.tf_min})"
            )
        n = tf_min // self.cfg.tf_min
        if len(self._bars) < n:
            return 0.0
        recientes = list(self._bars)[-n:]
        return sum(b.delta for b in recientes)

    # ------------------------------------------------------------------
    # Absorption -- reusa ofd.evaluar_absorcion() tal cual
    # ------------------------------------------------------------------
    def _maybe_detect_absorption(self, bar: BarData, previas: list[BarData]) -> None:
        if not self.cfg.absorption_enabled:
            return
        ventana_vol = [b.volume for b in previas[-self.cfg.absorption_lookback_bars:]]
        rango = bar.high - bar.low
        es_absorcion = ofd.evaluar_absorcion(
            bar.volume, bar.close, rango, bar.delta, ventana_vol,
            self.cfg.absorption_percentile_threshold, self.cfg.absorption_flip_ratio_threshold,
        )
        if not es_absorcion:
            return
        volume_usd = bar.volume * bar.close
        if volume_usd < self.cfg.absorption_min_volume_usd:
            return
        # delta<0 (venta agresiva neta) absorbida sin que el precio caiga ->
        # bullish (LONG). delta>0 absorbido sin que el precio suba -> bearish (SHORT).
        side = "LONG" if bar.delta < 0 else "SHORT"
        self._absorption_zones.append(AbsorptionZone(
            ts_ms=bar.ts_ms, tf_min=self.cfg.tf_min, price=bar.close, side=side,
            volume=bar.volume, delta=bar.delta, volume_usd=round(volume_usd, 2),
        ))

    def get_absorption_zones(self) -> list[AbsorptionZone]:
        return list(self._absorption_zones)

    # ------------------------------------------------------------------
    # Exhaustion -- ofd.evaluar_exhaustion()
    # ------------------------------------------------------------------
    def _maybe_detect_exhaustion(self, bar: BarData, previas: list[BarData]) -> None:
        if not self.cfg.exhaustion_enabled:
            return
        n = self.cfg.exhaustion_lookback_bars
        if len(previas) < n - 1:
            return
        ventana = previas[-(n - 1):] + [bar]
        closes = [b.close for b in ventana]
        deltas = [b.delta for b in ventana]
        volumes = [b.volume for b in ventana]
        direction = ofd.evaluar_exhaustion(
            closes, deltas, volumes,
            self.cfg.exhaustion_delta_divergence_pct, self.cfg.exhaustion_volume_decay_pct,
        )
        if direction is None:
            return

        prev_deltas = deltas[:-1]
        prev_volumes = volumes[:-1]
        avg_volume = sum(prev_volumes) / len(prev_volumes) if prev_volumes else 0.0
        volume_decay_pct = (1.0 - bar.volume / avg_volume) if avg_volume > 0 else 0.0
        if direction == "SHORT":
            base = [d for d in prev_deltas if d > 0]
        else:
            base = [d for d in prev_deltas if d < 0]
        avg_delta = sum(base) / len(base) if base else 0.0
        delta_divergence_pct = (1.0 - bar.delta / avg_delta) if avg_delta not in (0.0,) else 0.0

        self._exhaustion_signals.append(ExhaustionSignal(
            ts_ms=bar.ts_ms, tf_min=self.cfg.tf_min, price=bar.close, direction=direction,
            delta_divergence_pct=round(delta_divergence_pct, 4),
            volume_decay_pct=round(volume_decay_pct, 4),
        ))

    def get_exhaustion_signals(self) -> list[ExhaustionSignal]:
        return list(self._exhaustion_signals)

    # ------------------------------------------------------------------
    # Delta Trap -- ofd.evaluar_stop_run() ("Stop Run" en produccion,
    # mismo concepto/formula, otro nombre en el vocabulario de OrderFlow)
    # ------------------------------------------------------------------
    def _maybe_detect_delta_trap(self, bar: BarData, previas: list[BarData]) -> None:
        if not self.cfg.delta_trap_enabled:
            return
        ventana = previas[-self.cfg.delta_trap_lookback_bars:]
        if len(ventana) < self.cfg.delta_trap_min_bars_required:
            return
        high_n = max(b.high for b in ventana)
        low_n = min(b.low for b in ventana)
        direction = ofd.evaluar_stop_run(bar.high, bar.low, bar.close, bar.delta, high_n, low_n)
        if direction is None:
            return
        excursion = (bar.high - high_n) if direction == "SHORT" else (low_n - bar.low)
        self._delta_traps.append(DeltaTrap(
            ts_ms=bar.ts_ms, tf_min=self.cfg.tf_min, price=bar.close, direction=direction,
            price_excursion_ticks=round(excursion / self.cfg.tick_size, 2),
        ))

    def get_delta_traps(self) -> list[DeltaTrap]:
        return list(self._delta_traps)

    # ------------------------------------------------------------------
    # Footprint -- volume profile APROXIMADO (rolling window, no intrabar
    # real) via ofd.volume_profile(), reusado tal cual.
    # ------------------------------------------------------------------
    def _update_footprint(self, bar: BarData, previas: list[BarData]) -> None:
        self._fp_lows.append(bar.low)
        self._fp_highs.append(bar.high)
        self._fp_closes.append(bar.close)
        self._fp_vols.append(bar.volume)

        vp = ofd._volume_profile_core(
            np.fromiter(self._fp_lows, dtype=float), np.fromiter(self._fp_highs, dtype=float),
            np.fromiter(self._fp_closes, dtype=float), np.fromiter(self._fp_vols, dtype=float),
            bins=self.cfg.footprint_bins, value_area_pct=self.cfg.footprint_va_pct,
        )
        total_vol = sum(self._fp_vols)
        vwap = (sum(c * v for c, v in zip(self._fp_closes, self._fp_vols)) / total_vol) if total_vol > 0 else bar.close
        self._last_footprint = FootprintData(
            ts_ms=bar.ts_ms, tf_min=self.cfg.tf_min, poc=vp["poc"], vah=vp["vah"], val=vp["val"],
            vwap=round(vwap, 2), lookback_bars=len(self._fp_closes),
        )

    def get_footprint(self, tf_min: int) -> Optional[FootprintData]:
        if tf_min != self.cfg.tf_min:
            raise ValueError(f"tf_min={tf_min} no coincide con el tf_min base del engine ({self.cfg.tf_min})")
        return self._last_footprint

    # ------------------------------------------------------------------
    # Stack Imbalance -- NO calculable con este dataset, ver docstring del
    # modulo y de StackImbalance en schemas/market.py. Siempre None, no se
    # inventa un valor.
    # ------------------------------------------------------------------
    def get_stack_imbalance(self) -> Optional[StackImbalance]:
        return None

    # ------------------------------------------------------------------
    def snapshot(self) -> OrderFlowSnapshot:
        last = self._bars[-1] if self._bars else None
        five_tf = self.cfg.tf_min * 5
        try:
            delta_5 = self.get_delta(five_tf)
        except ValueError:
            delta_5 = 0.0
        return OrderFlowSnapshot(
            ts_ms=last.ts_ms if last is not None else 0,
            symbol=self.symbol,
            cvd=self._cvd,
            delta_1m=self.get_delta(self.cfg.tf_min) if self._bars else 0.0,
            delta_5m=delta_5,
            footprint_1m=self._last_footprint,
            absorption_zones=self.get_absorption_zones(),
            exhaustion_signals=self.get_exhaustion_signals(),
            delta_traps=self.get_delta_traps(),
            stack_imbalance=self.get_stack_imbalance(),
        )
