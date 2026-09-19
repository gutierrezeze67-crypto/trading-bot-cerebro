from types import SimpleNamespace

import pytest

from src.experts.scalping_expert import ScalpingExpert
from src.scalping_live import LiveBracketManager
from src.scalping_rules import Bracket, BracketParams, EntryGate, split_qty
import src.order_flow_signal as ofs_live

T0 = 1_000_000.0
PARAMS = BracketParams(be_buffer=100.0, time_stop_tp1_min=10, time_stop_close_min=30)


class FakeExecutor:
    def __init__(self):
        self.positions: dict[int, SimpleNamespace] = {}
        self.bid = self.ask = 0.0
        self.close_info: dict[int, dict] = {}
        self.calls: list[tuple] = []

    def get_position_state(self, ticket):
        return ("OPEN", self.positions[ticket]) if ticket in self.positions else ("CLOSED", None)

    def list_own_positions(self):
        return list(self.positions.values())

    def get_bid_ask(self, symbol=None):
        return self.bid, self.ask

    def close_position(self, ticket, deviation=None, volume=None):
        pos = self.positions[ticket]
        self.calls.append(("close", ticket, volume))
        if volume is None or volume >= pos.volume - 1e-9:
            del self.positions[ticket]
        else:
            pos.volume = round(pos.volume - volume, 8)
        return SimpleNamespace(success=True, error="")

    def modify_position(self, ticket, sl=None, tp=None):
        pos = self.positions[ticket]
        self.calls.append(("modify", ticket, sl, tp))
        if sl is not None:
            pos.sl = sl
        if tp is not None:
            pos.tp = tp
        return SimpleNamespace(success=True, error="")

    def get_close_info(self, ticket):
        return self.close_info.get(ticket)


class Clock:
    def __init__(self):
        self.now = T0

    def __call__(self):
        return self.now


def _long_position(ticket=1, volume=0.05):
    return SimpleNamespace(ticket=ticket, symbol="BTCUSDz", type=0, volume=volume, price_open=100_000.0,
                           sl=99_750.0, tp=101_000.0, magic=999999)


def _setup(tmp_path, volume=0.05):
    ex, clock, gate = FakeExecutor(), Clock(), EntryGate()
    ex.positions[1] = _long_position(volume=volume)
    ex.bid = ex.ask = 100_000.0
    mgr = LiveBracketManager(ex, gate, PARAMS, tmp_path / "state.json", clock=clock)
    mgr.register(SimpleNamespace(ticket=1, tp=100_600.0, volume=volume, price=100_000.0))
    mgr._sync_step()
    return ex, clock, gate, mgr


def test_split_qty_matches_backtest_rounding():
    assert split_qty(0.05) == pytest.approx(0.02)   # round(2.5) -> 2
    assert split_qty(0.02) == pytest.approx(0.01)
    assert split_qty(0.01) == 0.01                  # 50% redondea a 0 -> posicion entera en TP1
    assert split_qty(1.0, step=0.001) == pytest.approx(0.5)


def test_tp1_partial_close_then_breakeven(tmp_path):
    ex, clock, gate, mgr = _setup(tmp_path)
    assert len(mgr.brackets) == 1 and not gate.allow(14, clock.now, position_open=False)[0] is False
    ex.bid = ex.ask = 100_601.0
    clock.now += 60
    mgr._sync_step()
    assert ("close", 1, pytest.approx(0.02)) in [(c[0], c[1], c[2]) for c in ex.calls]
    assert ex.positions[1].volume == pytest.approx(0.03)
    assert ex.positions[1].sl == 100_100.0 and ex.positions[1].tp == 101_000.0
    n = len(ex.calls)
    mgr._sync_step()
    assert len(ex.calls) == n  # sin acciones repetidas


def test_whole_position_when_lot_too_small_sets_tp1_on_broker(tmp_path):
    ex, clock, gate, mgr = _setup(tmp_path, volume=0.01)
    assert ("modify", 1, None, 100_600.0) in ex.calls
    assert ex.positions[1].tp == 100_600.0


def test_time_stop_tp1_with_price_above_be_moves_stop(tmp_path):
    ex, clock, gate, mgr = _setup(tmp_path)
    ex.bid = ex.ask = 100_200.0
    clock.now += 10 * 60
    mgr._sync_step()
    assert ex.positions[1].volume == pytest.approx(0.03)
    assert ex.positions[1].sl == 100_100.0


def test_time_stop_tp1_with_price_below_be_closes_remainder(tmp_path):
    ex, clock, gate, mgr = _setup(tmp_path)
    ex.bid = ex.ask = 100_050.0  # por debajo de BE (100_100): el broker no admite ese SL
    clock.now += 10 * 60
    mgr._sync_step()
    assert 1 not in ex.positions


def test_time_stop_close_after_tp1(tmp_path):
    ex, clock, gate, mgr = _setup(tmp_path)
    ex.bid = ex.ask = 100_601.0
    clock.now += 60
    mgr._sync_step()
    ex.bid = ex.ask = 100_300.0
    clock.now += 30 * 60
    mgr._sync_step()
    assert 1 not in ex.positions


def test_sl_before_tp1_arms_cooldown_but_be_stop_does_not(tmp_path):
    ex, clock, gate, mgr = _setup(tmp_path)
    del ex.positions[1]
    ex.close_info[1] = {"reason": "SL", "pnl": -10.0}
    mgr._sync_step()
    assert gate.last_sl_exit_ts == clock.now and not mgr.brackets

    ex2, clock2, gate2, mgr2 = _setup(tmp_path / "..")
    ex2.bid = ex2.ask = 100_601.0
    clock2.now += 60
    mgr2._sync_step()
    del ex2.positions[1]
    ex2.close_info[1] = {"reason": "SL", "pnl": 1.0}
    mgr2._sync_step()
    assert gate2.last_sl_exit_ts is None


def test_state_survives_restart_and_orphans_are_adopted(tmp_path):
    ex, clock, gate, mgr = _setup(tmp_path)
    mgr2 = LiveBracketManager(ex, EntryGate(), PARAMS, tmp_path / "state.json", clock=clock)
    assert 1 in mgr2.brackets and mgr2.brackets[1].qty_tp1 == pytest.approx(0.02)

    ex.positions[7] = _long_position(ticket=7)
    mgr3 = LiveBracketManager(ex, EntryGate(), PARAMS, tmp_path / "otro.json", clock=clock)
    mgr3._sync_step()
    assert 7 in mgr3.brackets and mgr3.brackets[7].tp1 == pytest.approx(100_600.0)


def test_entry_gate_rules():
    g = EntryGate()
    assert g.allow(12, T0, False) == (False, "FUERA_SESION_13-17_UTC")
    assert g.allow(17, T0, False)[0] is False
    assert g.allow(13, T0, True) == (False, "POSICION_ABIERTA")
    assert g.allow(16, T0, False)[0] is True
    g.record_sl_exit(T0)
    assert g.allow(14, T0 + 10, False) == (False, "COOLDOWN_TRAS_SL")
    assert g.allow(14, T0 + 31, False)[0] is True
    for k in range(8):
        g.record_entry(T0 + 100 + k)
    assert g.allow(14, T0 + 200, False) == (False, "MAX_TRADES_POR_HORA")
    assert g.allow(14, T0 + 100 + 3601, False)[0] is True


def _payload(t, patron="abs", price=77_850.0):
    vela = {"t": t, "c": price, "delta": -5.0, "vol": 10.0, "imb": [], "abs": False, "iceberg": False, "div": False,
            "stop_run": None, "initiative_pullback": None, "breakout_vol": None, "liquidity_zone": None}
    if patron == "abs":
        vela["abs"] = True
    elif patron == "iceberg":
        vela["iceberg"] = True
    return {"htf_ready": True, "snapshot": {"htf": {"poc": 78_691.0, "vah": 79_600.0, "val": 77_834.0}, "ltf_1m": [vela]}}


class _RM:
    def calcular_tamano_posicion(self, *a):
        return 100.0


def _expert(gate=None, open_fn=None):
    ofs_live._ZONE_COOLDOWNS.clear()
    return ScalpingExpert(_RM(), gate=gate or EntryGate(), position_open_fn=open_fn)


def _snap(payload):
    return SimpleNamespace(scalping_payload=payload, equity_usdt=200.0)


def test_expert_signals_in_session_and_sends_tp2_to_broker():
    sig = _expert().analyze(_snap(_payload("14:30:00")))
    assert sig is not None and sig.pattern_name.value == "ABSORPTION"
    assert sig.metadata["exec_tp_price"] > sig.tp_price > sig.entry_price


def test_expert_blocks_outside_session_without_burning_zone_cooldown():
    e = _expert()
    assert e.analyze(_snap(_payload("09:30:00"))) is None
    assert ofs_live._ZONE_COOLDOWNS == {}  # decide() no se llamo


def test_expert_blocks_unvalidated_patterns():
    assert _expert().analyze(_snap(_payload("14:30:00", patron="iceberg"))) is None


def test_expert_blocks_when_position_open_and_evaluates_each_candle_once():
    assert _expert(open_fn=lambda: True).analyze(_snap(_payload("14:30:00"))) is None
    e = _expert()
    assert e.analyze(_snap(_payload("14:30:00"))) is not None
    assert e.analyze(_snap(_payload("14:30:00"))) is None  # misma vela: no re-evalua


def test_live_manager_keep_sl_mode_leaves_original_stop_after_time_stop(tmp_path):
    ex, clock, gate = FakeExecutor(), Clock(), EntryGate()
    ex.positions[1] = _long_position()
    ex.bid = ex.ask = 100_000.0
    params = BracketParams(be_buffer=100.0, time_stop_tp1_min=10, time_stop_close_min=30, time_stop_mode="keep_sl")
    mgr = LiveBracketManager(ex, gate, params, tmp_path / "s.json", clock=clock)
    mgr.register(SimpleNamespace(ticket=1, tp=100_600.0, volume=0.05, price=100_000.0))
    mgr._sync_step()
    ex.bid = ex.ask = 100_050.0
    clock.now += 10 * 60
    mgr._sync_step()
    assert ex.positions[1].volume == pytest.approx(0.03)
    assert ex.positions[1].sl == 99_750.0  # SL original intacto: el corredor sigue vivo
    assert not any(c[0] == "modify" and c[2] == 100_100.0 for c in ex.calls)


def _live_mode_manager(tmp_path, mode, volume=0.05):
    ex, clock, gate = FakeExecutor(), Clock(), EntryGate()
    ex.positions[1] = _long_position(volume=volume)
    ex.bid = ex.ask = 100_000.0
    params = BracketParams(be_buffer=100.0, time_stop_tp1_min=10, time_stop_close_min=30, time_stop_mode=mode)
    mgr = LiveBracketManager(ex, gate, params, tmp_path / f"{mode}.json", clock=clock)
    mgr.register(SimpleNamespace(ticket=1, tp=100_600.0, volume=volume, price=100_000.0))
    mgr._sync_step()
    return ex, clock, mgr


def test_live_close_all_mode_closes_everything_at_10_min(tmp_path):
    ex, clock, mgr = _live_mode_manager(tmp_path, "close_all")
    ex.bid = ex.ask = 100_050.0
    clock.now += 10 * 60
    mgr._sync_step()
    assert 1 not in ex.positions
    assert [c for c in ex.calls if c[0] == "close"] == [("close", 1, None)]  # un solo cierre total


def test_live_conditional_be_mode_closes_rest_when_be_invalid_and_keeps_it_when_valid(tmp_path):
    ex, clock, mgr = _live_mode_manager(tmp_path, "conditional_be")
    ex.bid = ex.ask = 100_050.0  # BE (100_100) inejecutable
    clock.now += 10 * 60
    mgr._sync_step()
    assert 1 not in ex.positions

    ex, clock, mgr = _live_mode_manager(tmp_path, "conditional_be")
    ex.bid = ex.ask = 100_200.0  # BE valido
    clock.now += 10 * 60
    mgr._sync_step()
    assert ex.positions[1].volume == pytest.approx(0.03) and ex.positions[1].sl == 100_100.0
