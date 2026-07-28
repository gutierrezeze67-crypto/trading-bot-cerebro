"""Adaptador sobre HTFFundingBrain.decide() (src/brain_htf_funding.py -- la
version VIVA, no la copia congelada en strategies/htf_funding_btc/src/, ver
docstring de esa copia). No reescribe logica, solo traduce su dict de salida."""
from __future__ import annotations

from src.brain_htf_funding import HTFFundingBrain, HTFParams

from src.experts.base_expert import BaseExpert
from src.schemas.market import MarketSnapshot
from src.schemas.signals import PatternName, UnifiedSignal


class SwingExpert(BaseExpert):
    def __init__(self, params: HTFParams | None = None, asset_cfg: dict | None = None) -> None:
        self.brain = HTFFundingBrain(params=params, asset_cfg=asset_cfg)

    def analyze(self, snapshot: MarketSnapshot) -> UnifiedSignal | None:
        raw = self.brain.decide(snapshot.htf_vela, snapshot.htf_context, snapshot.ts_ms)
        if raw is None:
            return None
        return self._to_unified(raw)

    @staticmethod
    def _to_unified(raw: dict) -> UnifiedSignal:
        return UnifiedSignal(
            strategy_type="swing",
            pattern_name=PatternName(raw["setup_type"]),
            direction=raw["decision"],
            entry_price=raw["entry_price"],
            sl_price=raw["stop_loss_price"],
            tp_price=raw["tp1_price"],
            confidence=raw["conviction"] / 10.0,
            reasoning=raw.get("management_notes", ""),
            target_rr=raw.get("rr_net") or 1.0,
            metadata={
                "tp1_price": raw["tp1_price"],
                "tp2_price": raw["tp2_price"],
                "tp1_size_pct": raw.get("tp1_size_pct"),
                "atr14_15m": raw.get("atr14_15m"),
                "cost_bps": raw.get("cost_bps"),
                "hold_hours_est": raw.get("hold_hours_est"),
            },
        )
