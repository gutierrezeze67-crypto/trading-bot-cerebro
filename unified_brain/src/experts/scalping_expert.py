"""Adaptador sobre src/order_flow_signal.decide() -- NO reescribe logica de
deteccion de patrones, solo traduce su dict de salida al contrato UnifiedSignal."""
from __future__ import annotations

from typing import Any

import structlog

from src.order_flow_signal import decide as scalping_decide

from src.experts.base_expert import BaseExpert
from src.schemas.market import MarketSnapshot
from src.schemas.signals import PatternName, UnifiedSignal

logger = structlog.get_logger(__name__)


class ScalpingExpert(BaseExpert):
    def __init__(self, risk_manager: Any) -> None:
        """risk_manager: instancia real de src.risk_manager.RiskManager (o
        cualquier duck-type con calcular_tamano_posicion(conviction, entry,
        sl, equity) -> float -- decide() solo llama a ese metodo)."""
        self.risk_manager = risk_manager
        self._last_diag_candle: Any = None

    def analyze(self, snapshot: MarketSnapshot) -> UnifiedSignal | None:
        raw = scalping_decide(snapshot.scalping_payload, snapshot.equity_usdt, self.risk_manager)
        if raw["decision"] == "NO_TRADE":
            self._log_reject(snapshot.scalping_payload, raw)
            return None
        return self._to_unified(raw)

    def _log_reject(self, payload: dict, raw: dict) -> None:
        try:
            snap = payload.get("snapshot", {})
            htf = snap.get("htf", {})
            ltf = snap.get("ltf_1m") or []
            candle = ltf[-1].get("t") if ltf else None
            if candle is None or candle == self._last_diag_candle:
                return
            self._last_diag_candle = candle
            precio = ltf[-1]["c"]
            zonas = {k: htf.get(k) for k in ("poc", "vah", "val", "swing_high_h4", "swing_low_h4") if htf.get(k)}
            cercana = min(zonas.items(), key=lambda kv: abs(precio - kv[1]), default=None)
            dist = f"{abs(precio - cercana[1]) / precio:.3%}" if cercana else "n/a"
            logger.info(
                "scalping_reject",
                motivo=raw.get("ltf_trigger_detail"),
                precio=precio,
                zona_cercana=cercana[0] if cercana else None,
                dist=dist,
                htf_ready=payload.get("htf_ready"),
            )
        except Exception:
            pass

    @staticmethod
    def _to_unified(raw: dict) -> UnifiedSignal:
        tp_levels = raw.get("tp_levels") or []
        tp_price = tp_levels[0]["price"] if tp_levels else raw["entry_price"]
        risk_metrics = raw.get("risk_metrics", {})
        reasoning = f"{raw.get('htf_context_1line', '')} | {raw.get('ltf_trigger_detail', '')}".strip(" |")

        return UnifiedSignal(
            strategy_type="scalping",
            pattern_name=PatternName(raw["setup_type"]),
            direction=raw["decision"],
            entry_price=raw["entry_price"],
            sl_price=raw["stop_loss_price"],
            tp_price=tp_price,
            confidence=raw["conviction"] / 10.0,
            reasoning=reasoning,
            target_rr=risk_metrics.get("r_multiple_tp1") or 1.0,
            metadata={
                "tp_levels": tp_levels,
                "position_size_usdt": raw.get("position_size_usdt"),
                "confluence_checklist": raw.get("confluence_checklist"),
                "risk_metrics": risk_metrics,
                "management_notes": raw.get("management_notes"),
            },
        )
