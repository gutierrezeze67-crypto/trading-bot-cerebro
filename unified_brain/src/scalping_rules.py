"""Reglas de scalping VALIDADAS en backtest, portadas al vivo sin tocar decide().

Fuente de verdad: backtest_standard_account.py (simular_ventana /
_gestionar_posicion) y results/btc_standard_account_wf_exness_zero_prod_
exness_zero_20260831_085543.json (311 trades, WR 68.49%, PF 1.8017).
Este modulo NO tiene I/O: es la logica pura que usan el ScalpingExpert (guardas
de entrada) y el LiveBracketManager (gestion de la posicion). Cualquier cambio
aca debe seguir dando 0 diferencias en scripts/parity_check_scalping.py (con y sin --small).
"""
from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Optional

# Constantes con las que se corrio el backtest validado (override LOCAL en el
# backtest; aca se aplican solo en el proceso de scalping, el proceso swing no
# las ve porque es otro proceso con su propio `config.constants`).
VALIDATED_CONSTANTS: dict[str, float] = {
    "TP1_PIPS": 600.0,
    "TP2_PIPS": 1000.0,
    "SL_PIPS": 250.0,
    "BREAKEVEN_BUFFER_PIPS": 100.0,
    "TIME_STOP_TP1_MINUTES": 10,
    "TIME_STOP_CLOSE_MINUTES": 30,
    "HTF_ZONE_DISTANCE_PCT": 0.002,
    "INSTITUTIONAL_ZONE_TOLERANCE_PCT": 0.0015,
    "ZONE_COOLDOWN_HOURS": 0.5,
}

ALLOWED_PATTERNS = frozenset({"ABSORPTION", "INITIATIVE_PULLBACK", "STOP_RUN"})
SESSION_UTC = (13, 17)  # [13, 17) UTC, hora de apertura de la vela de señal
MAX_TRADES_PER_HOUR = 8
COOLDOWN_AFTER_SL_SEC = 30
TP1_SIZE_PCT = 0.5
STEP_SIZE = 0.01  # paso real de volumen (BTC) en Exness


def apply_validated_constants(constants_module) -> dict:
    """Setea VALIDATED_CONSTANTS sobre config.constants; devuelve los valores
    previos (para logs/tests)."""
    previos = {}
    for name, value in VALIDATED_CONSTANTS.items():
        previos[name] = getattr(constants_module, name, None)
        setattr(constants_module, name, value)
    return previos


def split_qty(qty_total: float, tp1_pct: float = TP1_SIZE_PCT, step: float = STEP_SIZE) -> float:
    """Mismo redondeo que simular_ventana (linea 737): si el 50% redondea a 0
    (lote chico), TODA la posicion sale en TP1."""
    return round((qty_total * tp1_pct) / step) * step or qty_total


@dataclass
class Event:
    kind: str  # SL | TP1_PARCIAL | BE_STOP | TP2 | TIME_STOP_TP1 | TIME_STOP_CLOSE
    price: float
    qty: float


@dataclass
class BracketParams:
    be_buffer: float = VALIDATED_CONSTANTS["BREAKEVEN_BUFFER_PIPS"]
    time_stop_tp1_min: float = VALIDATED_CONSTANTS["TIME_STOP_TP1_MINUTES"]
    time_stop_close_min: float = VALIDATED_CONSTANTS["TIME_STOP_CLOSE_MINUTES"]
    # Que hacer al llegar al time-stop 1 (sin TP1):
    #   legacy_be      = replica EXACTA del backtest validado: cierra el 50% y pasa el SL a
    #                    BE, que el simulador 'rellena' aunque el precio ya este por debajo
    #                    (fill fantasma, NO ejecutable). Solo para paridad/comparacion.
    #   keep_sl        = cierra el 50%; el resto conserva su SL original.
    #   close_all      = cierra el 100% a mercado (Propuesta 1).
    #   conditional_be = cierra el 50%; si el BE es un stop valido (precio mas alla) el SL
    #                    pasa a BE, si no cierra el resto a mercado (Propuesta 2).
    time_stop_mode: str = "legacy_be"


class Bracket:
    """Replica exacta de _gestionar_posicion (rama sin trailing) del backtest.
    on_bar() recibe high/low/close de la ventana observada (una vela de 1m en
    el backtest; un tick con high=low=close=precio en vivo)."""

    def __init__(self, direction: str, entry: float, sl: float, tp1: float, tp2: float,
                 qty_total: float, qty_tp1: float, entry_ts: float,
                 params: Optional[BracketParams] = None, tp1_hit: bool = False,
                 qty_open: Optional[float] = None) -> None:
        self.direction = direction
        self.entry = entry
        self.sl = sl
        self.tp1 = tp1
        self.tp2 = tp2
        self.qty_total = qty_total
        self.qty_tp1 = qty_tp1
        self.qty_open = qty_total if qty_open is None else qty_open
        self.entry_ts = entry_ts  # epoch segundos
        self.tp1_hit = tp1_hit
        self.closed = False
        self.params = params or BracketParams()

    @property
    def be_price(self) -> float:
        return self.entry + self.params.be_buffer if self.direction == "LONG" else self.entry - self.params.be_buffer

    def _post_tp1(self) -> None:
        self.sl = self.be_price

    def _be_wrong_side(self, price: float) -> bool:
        """True si el BE no es un stop valido: en LONG el SL tiene que quedar por debajo del precio."""
        return self.be_price >= price if self.direction == "LONG" else self.be_price <= price

    def mark_partial(self, kind: str = "TP1_PARCIAL") -> None:
        """Aplica el efecto de un TP1_PARCIAL/TIME_STOP_TP1 ya ejecutado en el broker."""
        self.tp1_hit = True
        self.qty_open = self.qty_total - self.qty_tp1
        if kind == "TP1_PARCIAL" or self.params.time_stop_mode in ("legacy_be", "conditional_be"):
            self._post_tp1()

    def to_dict(self) -> dict:
        return {
            "direction": self.direction, "entry": self.entry, "sl": self.sl, "tp1": self.tp1, "tp2": self.tp2,
            "qty_total": self.qty_total, "qty_tp1": self.qty_tp1, "qty_open": self.qty_open,
            "entry_ts": self.entry_ts, "tp1_hit": self.tp1_hit,
        }

    @classmethod
    def from_dict(cls, d: dict, params: Optional[BracketParams] = None) -> "Bracket":
        return cls(d["direction"], d["entry"], d["sl"], d["tp1"], d["tp2"], d["qty_total"], d["qty_tp1"],
                   d["entry_ts"], params, tp1_hit=d["tp1_hit"], qty_open=d["qty_open"])

    def on_bar(self, ts: float, high: float, low: float, close: float) -> list[Event]:
        events: list[Event] = []
        hold_min = (ts - self.entry_ts) / 60
        long = self.direction == "LONG"

        if not self.tp1_hit:
            sl_hit, tp1_hit = (low <= self.sl, high >= self.tp1) if long else (high >= self.sl, low <= self.tp1)
            if sl_hit:
                events.append(Event("SL", self.sl, self.qty_open))
                self.closed = True
                return events
            if tp1_hit:
                events.append(Event("TP1_PARCIAL", self.tp1, self.qty_tp1))
                self.tp1_hit = True
                self.qty_open = self.qty_total - self.qty_tp1
                self._post_tp1()
        else:
            sl_hit, tp2_hit = (low <= self.sl, high >= self.tp2) if long else (high >= self.sl, low <= self.tp2)
            if sl_hit:
                events.append(Event("BE_STOP", self.sl, self.qty_open))
                self.closed = True
                return events
            if tp2_hit:
                events.append(Event("TP2", self.tp2, self.qty_open))
                self.closed = True
                return events

        if not self.tp1_hit and hold_min >= self.params.time_stop_tp1_min:
            mode = self.params.time_stop_mode
            if mode == "close_all":
                events.append(Event("TIME_STOP_10M", close, self.qty_open))
                self.closed = True
                return events
            events.append(Event("TIME_STOP_TP1", close, self.qty_tp1))
            self.qty_open = self.qty_total - self.qty_tp1
            self.tp1_hit = True
            if mode == "legacy_be":
                self._post_tp1()
            elif self.qty_open <= 1e-12:
                self.closed = True  # lote chico: el 'cierre del 50%' fue toda la posicion
            elif mode == "conditional_be":
                if self._be_wrong_side(close):
                    events.append(Event("TS10_BE_INVALID", close, self.qty_open))
                    self.closed = True
                else:
                    self._post_tp1()
        elif hold_min >= self.params.time_stop_close_min:
            events.append(Event("TIME_STOP_CLOSE", close, self.qty_open))
            self.closed = True
        return events


class EntryGate:
    """Guardas de entrada del backtest (simular_ventana lineas 665-690): una
    posicion a la vez, sesion 13-17 UTC, cooldown tras SL, tope de trades por
    hora. El limite de perdida diaria (2.5%) lo aplica el RiskEngine en vivo."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.entry_ts: list[float] = []
        self.last_sl_exit_ts: Optional[float] = None

    def allow(self, candle_hour_utc: int, now_ts: float, position_open: bool) -> tuple[bool, str]:
        if position_open:
            return False, "POSICION_ABIERTA"
        if not (SESSION_UTC[0] <= candle_hour_utc < SESSION_UTC[1]):
            return False, "FUERA_SESION_13-17_UTC"
        with self._lock:
            if self.last_sl_exit_ts is not None and (now_ts - self.last_sl_exit_ts) < COOLDOWN_AFTER_SL_SEC:
                return False, "COOLDOWN_TRAS_SL"
            self.entry_ts = [t for t in self.entry_ts if (now_ts - t) < 3600]
            if len(self.entry_ts) >= MAX_TRADES_PER_HOUR:
                return False, "MAX_TRADES_POR_HORA"
        return True, ""

    def record_entry(self, ts: float) -> None:
        with self._lock:
            self.entry_ts.append(ts)

    def record_sl_exit(self, ts: float) -> None:
        with self._lock:
            self.last_sl_exit_ts = ts

    def to_dict(self) -> dict:
        with self._lock:
            return {"entry_ts": list(self.entry_ts), "last_sl_exit_ts": self.last_sl_exit_ts}

    def load_dict(self, data: dict) -> None:
        with self._lock:
            self.entry_ts = [float(t) for t in data.get("entry_ts", [])]
            self.last_sl_exit_ts = data.get("last_sl_exit_ts")
