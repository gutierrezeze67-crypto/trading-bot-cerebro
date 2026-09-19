"""Adaptador sobre src/order_flow_signal.decide() -- NO reescribe logica de
deteccion de patrones, solo traduce su dict de salida al contrato UnifiedSignal.

Con `gate` (deployment en vivo) aplica ademas las guardas del backtest
validado (ver src/scalping_rules.py): una evaluacion por vela cerrada, una
posicion a la vez, sesion 13-17 UTC, cooldown tras SL, tope de trades por hora
y whitelist de patrones (ABSORPTION / INITIATIVE_PULLBACK / STOP_RUN). Sin
`gate` se comporta como el adaptador puro original."""
from __future__ import annotations

import time
from typing import Any, Callable, Optional

import structlog

from src.order_flow_signal import decide as scalping_decide

from src.experts.base_expert import BaseExpert
from src.scalping_rules import ALLOWED_PATTERNS, EntryGate
from src.schemas.market import MarketSnapshot
from src.schemas.signals import PatternName, UnifiedSignal

logger = structlog.get_logger(__name__)


class ScalpingExpert(BaseExpert):
    def __init__(self, risk_manager: Any, gate: Optional[EntryGate] = None,
                 position_open_fn: Optional[Callable[[], bool]] = None) -> None:
        """risk_manager: instancia real de src.risk_manager.RiskManager (o
        cualquier duck-type con calcular_tamano_posicion(conviction, entry,
        sl, equity) -> float -- decide() solo llama a ese metodo)."""
        self.risk_manager = risk_manager
        self.gate = gate
        self.position_open_fn = position_open_fn
        self._last_candle: Any = None

    def analyze(self, snapshot: MarketSnapshot) -> UnifiedSignal | None:
        payload = snapshot.scalping_payload
        if self.gate is None:
            raw = scalping_decide(payload, snapshot.equity_usdt, self.risk_manager)
            return None if raw["decision"] == "NO_TRADE" else self._to_unified(raw)

        ltf = (payload.get("snapshot") or {}).get("ltf_1m") or []
        if not ltf:
            return None
        candle = ltf[-1].get("t")
        if candle == self._last_candle:
            return None  # ya evaluada: el backtest decide una sola vez por vela cerrada
        self._last_candle = candle

        try:
            hour = int(str(candle)[:2])
        except ValueError:
            logger.warning("scalping_bad_candle_time", t=candle)
            return None

        position_open = bool(self.position_open_fn()) if self.position_open_fn else False
        ok, why = self.gate.allow(hour, time.time(), position_open)
        if not ok:
            self._log(candle, ltf[-1], payload, why)
            return None

        raw = scalping_decide(payload, snapshot.equity_usdt, self.risk_manager)
        if raw["decision"] == "NO_TRADE":
            self._log(candle, ltf[-1], payload, raw.get("ltf_trigger_detail"))
            return None
        if raw["setup_type"] not in ALLOWED_PATTERNS:
            self._log(candle, ltf[-1], payload, f"PATRON_NO_VALIDADO:{raw['setup_type']}")
            return None
        logger.info("scalping_signal", setup=raw["setup_type"], direction=raw["decision"], entry=raw["entry_price"], candle=candle)
        return self._to_unified(raw)

    def _log(self, candle: Any, last: dict, payload: dict, motivo: Any) -> None:
        try:
            htf = (payload.get("snapshot") or {}).get("htf") or {}
            precio = last["c"]
            zonas = {k: htf.get(k) for k in ("poc", "vah", "val", "swing_high_h4", "swing_low_h4") if htf.get(k)}
            cercana = min(zonas.items(), key=lambda kv: abs(precio - kv[1]), default=None)
            dist = f"{abs(precio - cercana[1]) / precio:.3%}" if cercana else "n/a"
            logger.info("scalping_reject", vela=candle, motivo=motivo, precio=precio,
                        zona_cercana=cercana[0] if cercana else None, dist=dist)
        except Exception:  # noqa: BLE001 - el log de diagnostico nunca puede romper la decision
            pass

    @staticmethod
    def _to_unified(raw: dict) -> UnifiedSignal:
        tp_levels = raw.get("tp_levels") or []
        tp_price = tp_levels[0]["price"] if tp_levels else raw["entry_price"]
        risk_metrics = raw.get("risk_metrics", {})
        reasoning = f"{raw.get('htf_context_1line', '')} | {raw.get('ltf_trigger_detail', '')}".strip(" |")
        metadata = {
            "tp_levels": tp_levels,
            "position_size_usdt": raw.get("position_size_usdt"),
            "confluence_checklist": raw.get("confluence_checklist"),
            "risk_metrics": risk_metrics,
            "management_notes": raw.get("management_notes"),
        }
        if len(tp_levels) >= 2:
            # El broker cuelga TP2 (corredor); el 50% en TP1 lo gestiona LiveBracketManager.
            metadata["exec_tp_price"] = tp_levels[1]["price"]

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
            metadata=metadata,
        )
