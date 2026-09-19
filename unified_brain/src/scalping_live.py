"""Gestion en vivo de la posicion de scalping, segun las reglas validadas en
backtest (scalping_rules.Bracket): 50% en TP1, breakeven, corredor a TP2 y
time-stops. SL y TP2 viven en el broker (server-side); este manager solo actua
en TP1 parcial, time-stops y el movimiento a breakeven. Estado persistido en
disco para sobrevivir a los reinicios del watchdog."""
from __future__ import annotations

import asyncio
import copy
import json
import os
import threading
import time
from pathlib import Path
from typing import Any, Callable

import structlog

from src.scalping_rules import Bracket, BracketParams, EntryGate, VALIDATED_CONSTANTS, split_qty

logger = structlog.get_logger(__name__)

SL_SYNC_TOLERANCE = 0.5  # USD


class LiveBracketManager:
    def __init__(self, executor: Any, gate: EntryGate, params: BracketParams, state_path: str | Path,
                 interval_s: float = 2.0, clock: Callable[[], float] = time.time) -> None:
        self._clock = clock
        self.executor = executor
        self.gate = gate
        self.params = params
        self.state_path = Path(state_path)
        self.interval_s = interval_s
        self.brackets: dict[int, Bracket] = {}
        self._new: list[dict] = []
        self._lock = threading.Lock()
        self._loops = 0
        self._load_state()

    # ------------------------------------------------------------------
    # API para el orquestador / expert
    # ------------------------------------------------------------------

    def has_open_position(self) -> bool:
        with self._lock:
            return bool(self.brackets) or bool(self._new)

    def register(self, exec_result: Any) -> None:
        """on_trade_executed del orquestador: ticket/volume/price/sl reales del
        broker y tp = TP1 de la señal. TP2 se lee de la posicion (se envio como
        TP del broker)."""
        with self._lock:
            self._new.append({
                "ticket": int(exec_result.ticket), "tp1": float(exec_result.tp), "ts": self._clock(),
            })
        logger.info("scalping_trade_registered", ticket=exec_result.ticket, volume=exec_result.volume, price=exec_result.price)

    async def run(self) -> None:
        logger.info("scalping_manager_started", interval_s=self.interval_s, open=len(self.brackets))
        while True:
            try:
                await asyncio.to_thread(self._sync_step)
            except Exception as exc:  # noqa: BLE001 - un ciclo roto no puede tirar el manager
                logger.exception("scalping_manager_step_failed", error=str(exc))
            await asyncio.sleep(self.interval_s)

    # ------------------------------------------------------------------
    # Persistencia
    # ------------------------------------------------------------------

    def _save_state(self) -> None:
        with self._lock:
            data = {"brackets": {str(t): b.to_dict() for t, b in self.brackets.items()}, "gate": self.gate.to_dict()}
        tmp = self.state_path.with_suffix(".tmp")
        try:
            tmp.write_text(json.dumps(data), encoding="utf-8")
            os.replace(tmp, self.state_path)
        except OSError as exc:
            logger.warning("scalping_state_save_failed", error=str(exc))

    def _load_state(self) -> None:
        if not self.state_path.exists():
            return
        try:
            data = json.loads(self.state_path.read_text(encoding="utf-8"))
            self.brackets = {int(t): Bracket.from_dict(d, self.params) for t, d in data.get("brackets", {}).items()}
            self.gate.load_dict(data.get("gate", {}))
            logger.info("scalping_state_loaded", open=len(self.brackets))
        except Exception as exc:  # noqa: BLE001
            logger.warning("scalping_state_load_failed", error=str(exc))

    # ------------------------------------------------------------------
    # Ciclo (hilo de trabajo: todas las llamadas a MT5 son bloqueantes)
    # ------------------------------------------------------------------

    def _sync_step(self) -> None:
        with self._lock:
            new, self._new = self._new, []
        for n in new:
            self._adopt_new(n)
        if self._loops % 15 == 0:
            self._adopt_orphans()
        self._loops += 1
        for ticket, bracket in list(self.brackets.items()):
            self._manage_one(ticket, bracket)

    def _bracket_from_position(self, pos: Any, tp1: float, entry_ts: float) -> Bracket:
        direction = "LONG" if pos.type == 0 else "SHORT"  # mt5.POSITION_TYPE_BUY == 0
        qty_total = float(pos.volume)
        qty_tp1 = split_qty(qty_total)
        tp2 = float(pos.tp)
        b = Bracket(direction, float(pos.price_open), float(pos.sl), tp1, tp2, qty_total, qty_tp1, entry_ts, self.params)
        return b

    def _adopt_new(self, n: dict) -> None:
        ticket = n["ticket"]
        state, pos = self.executor.get_position_state(ticket)
        if state != "OPEN":
            logger.warning("scalping_new_position_not_open", ticket=ticket, state=state)
            return
        b = self._bracket_from_position(pos, n["tp1"], n["ts"])
        self.gate.record_entry(n["ts"])
        with self._lock:
            self.brackets[ticket] = b
        if b.qty_tp1 >= b.qty_total - 1e-9:
            # Lote chico: el 50% redondea a 0 -> toda la posicion sale en TP1
            # (misma regla que el backtest). TP1 pasa a ser el TP del broker.
            res = self.executor.modify_position(ticket, tp=b.tp1)
            logger.info("scalping_whole_position_tp1", ticket=ticket, tp1=b.tp1, ok=res.success, error=res.error)
        else:
            logger.info("scalping_bracket_armed", ticket=ticket, qty_total=b.qty_total, qty_tp1=b.qty_tp1, tp1=b.tp1, tp2=b.tp2, sl=b.sl)
        self._save_state()

    def _adopt_orphans(self) -> None:
        """Posiciones abiertas con nuestro magic que no estan registradas
        (proceso caido justo tras enviar la orden): se adoptan con las reglas
        estandar. TP1 se deduce de TP2 (TP2_PIPS - TP1_PIPS de distancia)."""
        positions = self.executor.list_own_positions()
        if not positions:
            return
        gap = VALIDATED_CONSTANTS["TP2_PIPS"] - VALIDATED_CONSTANTS["TP1_PIPS"]
        for pos in positions:
            if pos.ticket in self.brackets:
                continue
            tp1 = float(pos.tp) - gap if pos.type == 0 else float(pos.tp) + gap
            b = self._bracket_from_position(pos, tp1, self._clock())
            be = b.be_price
            if abs(b.sl - be) < SL_SYNC_TOLERANCE:
                b.tp1_hit = True
            with self._lock:
                self.brackets[pos.ticket] = b
            logger.warning("scalping_orphan_adopted", ticket=pos.ticket, direction=b.direction, volume=b.qty_total, tp1_hit=b.tp1_hit)
            self._save_state()

    def _finalize(self, ticket: int, b: Bracket) -> None:
        info = self.executor.get_close_info(ticket) or {}
        if info.get("reason") == "SL" and not b.tp1_hit:
            self.gate.record_sl_exit(self._clock())
        with self._lock:
            self.brackets.pop(ticket, None)
        logger.info("scalping_trade_closed", ticket=ticket, reason=info.get("reason"), pnl=info.get("pnl"), tp1_hit=b.tp1_hit)
        self._save_state()

    def _manage_one(self, ticket: int, b: Bracket) -> None:
        state, pos = self.executor.get_position_state(ticket)
        if state == "UNKNOWN":
            return
        if state == "CLOSED":
            self._finalize(ticket, b)
            return

        quote = self.executor.get_bid_ask(pos.symbol)
        if quote is None:
            return
        bid, ask = quote
        price = bid if b.direction == "LONG" else ask

        # SL a breakeven ya decidido pero no aplicado en el broker (fallo previo).
        if b.tp1_hit and abs(float(pos.sl) - b.sl) > SL_SYNC_TOLERANCE:
            self._apply_breakeven(ticket, b, pos, price)
            return

        sim = copy.copy(b)
        for ev in sim.on_bar(self._clock(), price, price, price):
            if ev.kind in ("SL", "BE_STOP", "TP2"):
                break  # los ejecuta el broker (SL/TP colgados), no por tick
            if ev.kind in ("TP1_PARCIAL", "TIME_STOP_TP1"):
                if not self._partial_close(ticket, b, pos, ev, price):
                    break
            elif ev.kind == "TS10_BE_INVALID":
                continue  # ya lo resolvio _apply_breakeven (BE del lado equivocado -> cierra el resto)
            elif ev.kind in ("TIME_STOP_CLOSE", "TIME_STOP_10M"):
                res = self.executor.close_position(ticket)
                logger.info("scalping_time_stop_close", ticket=ticket, ok=res.success, error=res.error)
                break

    def _partial_close(self, ticket: int, b: Bracket, pos: Any, ev: Any, price: float) -> bool:
        whole = ev.qty >= float(pos.volume) - 1e-9
        res = self.executor.close_position(ticket) if whole else self.executor.close_position(ticket, volume=ev.qty)
        logger.info("scalping_partial_close", ticket=ticket, kind=ev.kind, qty=ev.qty, whole=whole, ok=res.success, error=res.error)
        if not res.success:
            return False
        if whole:
            return True
        sl_before = b.sl
        b.mark_partial(ev.kind)
        if b.sl != sl_before:
            self._apply_breakeven(ticket, b, pos, price)
        self._save_state()
        return True

    def _apply_breakeven(self, ticket: int, b: Bracket, pos: Any, price: float) -> None:
        """Mueve el SL del corredor a entry +/- BE. Si el precio ya esta del
        lado equivocado del BE (time-stop con el precio bajo el BE), el broker
        no admite ese SL: se cierra el resto a mercado (lo mas cercano
        ejecutable a 'BE_STOP inmediato')."""
        wrong_side = b.be_price >= price if b.direction == "LONG" else b.be_price <= price
        if not wrong_side:
            res = self.executor.modify_position(ticket, sl=b.sl, tp=b.tp2)
            logger.info("scalping_breakeven_set", ticket=ticket, sl=b.sl, tp=b.tp2, ok=res.success, error=res.error)
            if res.success:
                return
        res = self.executor.close_position(ticket)
        logger.warning("scalping_breakeven_wrong_side_close", ticket=ticket, be=b.sl, price=price, ok=res.success, error=res.error)
