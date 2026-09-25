"""Gestion en vivo de la posicion de swing, segun las reglas que el backtest
SIEMPRE simulo (backtest_htf.py::_gestionar_posicion) pero que hasta
2026-09-25 nunca se conectaron al motor real: TP1 parcial (tp1_size_pct del
volumen), SL del resto a breakeven (entry +/- atr*be_buffer_atr_mult),
corredor hasta TP2, cierre a mercado si pasan max_hold_hours sin resolverse.

Antes de esto, SwingExpert mandaba tp=TP1 con el volumen COMPLETO: el broker
cerraba el 100% en el primer TP1 y el trade nunca podia llegar a TP2 --
confirmado en vivo que asi el sistema pierde en promedio (PF 0.75 sobre 280
trades reales reconstruidos, ver swing_expert.py). SL y TP2 viven en el
broker (server-side, puestos por SignalOrchestrator.execute_order); este
manager solo actua en el TP1 parcial, el pase a breakeven y el cierre por
tiempo -- mismo diseño que scalping_live.py::LiveBracketManager (que ya esta
en produccion para el otro bot), reescrito ACA sin importar nada de ese
modulo: swing y scalping son instalaciones separadas a proposito, ver
deploy/scalping/ y la rama swing-prod.

A diferencia del scalping, no hay ambiguedad de "fill fantasma" en el cierre
por tiempo: backtest_htf.py cierra el MAX_HOLD a fila["close"] (un precio de
mercado real, ejecutable), no a un nivel de breakeven que el precio ya paso
-- por eso este manager no necesita variantes de modo, solo replica la unica
regla real del backtest."""
from __future__ import annotations

import asyncio
import json
import os
import threading
import time
from pathlib import Path
from typing import Any, Callable

import structlog

logger = structlog.get_logger(__name__)


class Bracket:
    """Estado de una posicion de swing en gestion activa."""

    def __init__(
        self, direction: str, entry: float, sl_original: float, tp1: float, tp2: float,
        qty_total: float, qty_tp1: float, atr: float, be_buffer_atr_mult: float,
        entry_ts: float, tp1_hit: bool = False, qty_open: float | None = None,
    ) -> None:
        self.direction = direction
        self.entry = entry
        self.sl = sl_original
        self.tp1 = tp1
        self.tp2 = tp2
        self.qty_total = qty_total
        self.qty_tp1 = qty_tp1
        self.qty_open = qty_total if qty_open is None else qty_open
        self.atr = atr
        self.be_buffer_atr_mult = be_buffer_atr_mult
        self.entry_ts = entry_ts
        self.tp1_hit = tp1_hit

    @property
    def be_price(self) -> float:
        buffer = self.atr * self.be_buffer_atr_mult
        return self.entry + buffer if self.direction == "LONG" else self.entry - buffer

    def tp1_reached(self, bid: float, ask: float) -> bool:
        price = bid if self.direction == "LONG" else ask
        return price >= self.tp1 if self.direction == "LONG" else price <= self.tp1

    def hold_hours(self, now: float) -> float:
        return (now - self.entry_ts) / 3600.0

    def to_dict(self) -> dict:
        return {
            "direction": self.direction, "entry": self.entry, "sl": self.sl, "tp1": self.tp1, "tp2": self.tp2,
            "qty_total": self.qty_total, "qty_tp1": self.qty_tp1, "qty_open": self.qty_open,
            "atr": self.atr, "be_buffer_atr_mult": self.be_buffer_atr_mult,
            "entry_ts": self.entry_ts, "tp1_hit": self.tp1_hit,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Bracket":
        return cls(
            d["direction"], d["entry"], d["sl"], d["tp1"], d["tp2"], d["qty_total"], d["qty_tp1"],
            d["atr"], d["be_buffer_atr_mult"], d["entry_ts"], tp1_hit=d["tp1_hit"], qty_open=d["qty_open"],
        )


class SwingBracketManager:
    def __init__(
        self, executor: Any, max_hold_hours: float, be_buffer_atr_mult: float, state_path: str | Path,
        interval_s: float = 5.0, clock: Callable[[], float] = time.time,
    ) -> None:
        self._clock = clock
        self.executor = executor
        self.max_hold_hours = max_hold_hours
        self.be_buffer_atr_mult = be_buffer_atr_mult
        self.state_path = Path(state_path)
        self.interval_s = interval_s
        self.brackets: dict[int, Bracket] = {}
        self._new: list[dict] = []
        self._lock = threading.Lock()
        self._load_state()

    # ------------------------------------------------------------------
    # API para el orquestador / expert
    # ------------------------------------------------------------------

    def register(self, exec_result: Any, signal: Any) -> None:
        """on_trade_executed(exec_result, signal) del orquestador. El ticket
        real y la data de la SEÑAL (tp1/tp2/atr/tp1_size_pct -- exec_result
        solo trae el tp que quedo puesto en el broker, que ahora es TP2, no
        alcanza para reconstruir el bracket)."""
        with self._lock:
            self._new.append({
                "ticket": int(exec_result.ticket),
                "tp1": float(signal.metadata["tp1_price"]),
                "tp2": float(signal.metadata["tp2_price"]),
                "atr": float(signal.metadata["atr14_15m"]),
                "tp1_size_pct": float(signal.metadata.get("tp1_size_pct") or 0.5),
                "ts": self._clock(),
            })
        logger.info("swing_trade_registered", ticket=exec_result.ticket, volume=exec_result.volume, price=exec_result.price)

    async def run(self) -> None:
        logger.info("swing_manager_started", interval_s=self.interval_s, open=len(self.brackets))
        while True:
            try:
                await asyncio.to_thread(self._sync_step)
            except Exception as exc:  # noqa: BLE001 - un ciclo roto no puede tirar el manager
                logger.exception("swing_manager_step_failed", error=str(exc))
            await asyncio.sleep(self.interval_s)

    # ------------------------------------------------------------------
    # Persistencia (sobrevive a reinicios del watchdog)
    # ------------------------------------------------------------------

    def _save_state(self) -> None:
        with self._lock:
            data = {"brackets": {str(t): b.to_dict() for t, b in self.brackets.items()}}
        tmp = self.state_path.with_suffix(".tmp")
        try:
            tmp.write_text(json.dumps(data), encoding="utf-8")
            os.replace(tmp, self.state_path)
        except OSError as exc:
            logger.warning("swing_state_save_failed", error=str(exc))

    def _load_state(self) -> None:
        if not self.state_path.exists():
            return
        try:
            data = json.loads(self.state_path.read_text(encoding="utf-8"))
            self.brackets = {int(t): Bracket.from_dict(d) for t, d in data.get("brackets", {}).items()}
            logger.info("swing_state_loaded", open=len(self.brackets))
        except Exception as exc:  # noqa: BLE001
            logger.warning("swing_state_load_failed", error=str(exc))

    # ------------------------------------------------------------------
    # Ciclo (hilo de trabajo: todas las llamadas a MT5 son bloqueantes)
    # ------------------------------------------------------------------

    def _sync_step(self) -> None:
        with self._lock:
            new, self._new = self._new, []
        for n in new:
            self._adopt_new(n)
        for ticket, bracket in list(self.brackets.items()):
            self._manage_one(ticket, bracket)

    def _adopt_new(self, n: dict) -> None:
        ticket = n["ticket"]
        state, pos = self.executor.get_position_state(ticket)
        if state != "OPEN":
            logger.warning("swing_new_position_not_open", ticket=ticket, state=state)
            return
        qty_total = float(pos.volume)
        qty_tp1 = round(qty_total * n["tp1_size_pct"], 2)
        if qty_tp1 <= 0 or qty_tp1 >= qty_total:
            # tp1_size_pct absurdo o lote muy chico para partir -- todo el
            # volumen sale en TP1 (equivalente a lo que ya hacia el sistema
            # viejo para ese caso puntual, no rompe nada nuevo).
            qty_tp1 = qty_total
        direction = "LONG" if pos.type == 0 else "SHORT"  # mt5.POSITION_TYPE_BUY == 0
        entry_ts = float(getattr(pos, "time", 0)) or n["ts"]
        b = Bracket(
            direction=direction, entry=float(pos.price_open), sl_original=float(pos.sl),
            tp1=n["tp1"], tp2=n["tp2"], qty_total=qty_total, qty_tp1=qty_tp1,
            atr=n["atr"], be_buffer_atr_mult=self.be_buffer_atr_mult, entry_ts=entry_ts,
        )
        with self._lock:
            self.brackets[ticket] = b
        logger.info("swing_bracket_armed", ticket=ticket, direction=direction, qty_total=qty_total, qty_tp1=qty_tp1, tp1=b.tp1, tp2=b.tp2)
        self._save_state()

    def _finalize(self, ticket: int, b: Bracket) -> None:
        info = self.executor.get_close_info(ticket) or {}
        with self._lock:
            self.brackets.pop(ticket, None)
        logger.info("swing_trade_closed", ticket=ticket, reason=info.get("reason"), pnl=info.get("pnl"), tp1_hit=b.tp1_hit)
        self._save_state()

    def _manage_one(self, ticket: int, b: Bracket) -> None:
        state, pos = self.executor.get_position_state(ticket)
        if state == "UNKNOWN":
            return
        if state == "CLOSED":
            self._finalize(ticket, b)
            return

        now = self._clock()
        if b.hold_hours(now) >= self.max_hold_hours:
            res = self.executor.close_position(ticket)
            logger.info("swing_max_hold_close", ticket=ticket, hold_hours=b.hold_hours(now), ok=res.success, error=res.error)
            return  # el proximo ciclo detecta CLOSED y finaliza

        if b.tp1_hit:
            return  # ya paso a breakeven, el resto lo resuelve el broker (SL en BE o TP2)

        quote = self.executor.get_bid_ask(pos.symbol)
        if quote is None:
            return
        bid, ask = quote
        if not b.tp1_reached(bid, ask):
            return

        whole = b.qty_tp1 >= float(pos.volume) - 1e-9
        res = self.executor.close_position(ticket) if whole else self.executor.close_position(ticket, volume=b.qty_tp1)
        logger.info("swing_tp1_partial", ticket=ticket, qty=b.qty_tp1, whole=whole, ok=res.success, error=res.error)
        if not res.success:
            return
        if whole:
            return  # se cerro entera en TP1, no queda corredor -- el proximo ciclo detecta CLOSED
        b.tp1_hit = True
        b.qty_open = b.qty_total - b.qty_tp1
        mod = self.executor.modify_position(ticket, sl=b.be_price)
        logger.info("swing_breakeven_set", ticket=ticket, sl=b.be_price, ok=mod.success, error=mod.error)
        self._save_state()
