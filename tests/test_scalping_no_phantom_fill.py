"""Task 1: los modos ejecutables nunca 'rellenan' un stop/limite a un precio que el mercado no ofrecio."""
import pytest

from src.scalping_rules import Bracket, BracketParams

ENTRY, T0 = 100_000.0, 1_000_000.0


def _long(mode, qty_total=0.05, qty_tp1=0.02):
    params = BracketParams(be_buffer=100.0, time_stop_tp1_min=10, time_stop_close_min=30, time_stop_mode=mode)
    return Bracket("LONG", ENTRY, ENTRY - 250, ENTRY + 600, ENTRY + 1000, qty_total, qty_tp1, T0, params)


def _bar(minute, o, h, l, c):
    return {"ts": T0 + minute * 60, "high": h, "low": l, "close": c}


def _run(b, bars):
    fills = []
    for bar in bars:
        for ev in b.on_bar(bar["ts"], bar["high"], bar["low"], bar["close"]):
            fills.append((ev, bar))
        if b.closed:
            break
    return fills


# minuto 10: el precio cierra en entrada+50 (< BE = entrada+100); minuto 11: cae mas
BARS = [
    _bar(10, ENTRY + 50, ENTRY + 55, ENTRY + 45, ENTRY + 50),
    _bar(11, ENTRY + 50, ENTRY + 52, ENTRY + 20, ENTRY + 30),
    _bar(30, ENTRY + 30, ENTRY + 35, ENTRY + 25, ENTRY + 30),
]


def test_legacy_mode_reproduces_the_phantom_fill():
    """Documenta el artefacto del backtest validado: BE_STOP a entrada+100 con el precio en entrada+20..52."""
    fills = _run(_long("legacy_be"), BARS)
    ev, bar = fills[-1]
    assert ev.kind == "BE_STOP" and ev.price == ENTRY + 100
    assert ev.price > bar["high"]  # fill por encima de todo lo que el mercado ofrecio en esa vela


@pytest.mark.parametrize("mode", ["keep_sl", "close_all", "conditional_be"])
def test_no_phantom_be_fill(mode):
    fills = _run(_long(mode), BARS)
    assert fills, "debia cerrar algo al time-stop"
    assert fills[0][0].price == ENTRY + 50  # fill real = cierre de la vela 10, NO el nivel de BE
    for ev, bar in fills:
        assert bar["low"] <= ev.price <= bar["high"], f"{mode}: fill {ev.kind}@{ev.price} fuera del rango de su vela"


def test_close_all_closes_whole_position_in_one_event():
    fills = _run(_long("close_all"), BARS)
    assert [e.kind for e, _ in fills] == ["TIME_STOP_10M"] and fills[0][0].qty == pytest.approx(0.05)


def test_conditional_be_closes_rest_when_be_invalid_and_moves_stop_when_valid():
    kinds = [e.kind for e, _ in _run(_long("conditional_be"), BARS)]
    assert kinds == ["TIME_STOP_TP1", "TS10_BE_INVALID"]

    b = _long("conditional_be")
    _run(b, [_bar(10, ENTRY + 150, ENTRY + 160, ENTRY + 140, ENTRY + 150)])
    assert not b.closed and b.sl == ENTRY + 100  # BE valido: el corredor sigue con stop en BE


def test_lot_too_small_for_partial_closes_everything_in_every_executable_mode():
    for mode in ("keep_sl", "conditional_be"):
        b = _long(mode, qty_total=0.01, qty_tp1=0.01)
        _run(b, BARS[:1])
        assert b.closed, mode
