import asyncio
from types import SimpleNamespace

from src.experts.base_expert import BaseExpert, NullExpert
from src.experts.scalping_expert import ScalpingExpert
from src.risk.risk_engine import RiskEngine
from src.router.deterministic_router import HoldReason, RouterDecision
from src.schemas.risk import RiskConfig
from src.services.signal_orchestrator import SignalOrchestrator


class _CountingExpert(BaseExpert):
    def __init__(self):
        self.calls = 0

    def analyze(self, snapshot):
        self.calls += 1
        return None


class _RecordingRouter:
    def __init__(self):
        self.scalp_seen = "unset"

    def route(self, account, scalp_signal, swing_signal, context):
        self.scalp_seen = scalp_signal
        return RouterDecision(action="hold", hold_reason=HoldReason.NO_PATTERN, detail="test")


async def _noop_emit(*_a, **_k):
    return None


async def _account():
    return SimpleNamespace(
        equity=200.0, balance=200.0, equity_start_of_day=200.0, free_margin=200.0,
        margin_level=None, daily_pnl_pct=0.0, trades_today=0,
    )


def _run_tick(enable_scalping: bool):
    scalping, swing, router = _CountingExpert(), NullExpert(), _RecordingRouter()
    orch = SignalOrchestrator(
        scalping_expert=scalping, swing_expert=swing, router=router,
        risk_engine=RiskEngine(RiskConfig()), dispatcher=None,
        account_state_provider=_account, emit=_noop_emit,
        build_router_context=lambda snap: None, enable_scalping=enable_scalping,
    )
    snap = SimpleNamespace(ts_ms=1, symbol="BTCUSDT", closed_timeframes=[])
    asyncio.run(orch.process_market_tick(snap))
    return scalping, router


def test_scalping_not_called_by_default():
    scalping, router = _run_tick(enable_scalping=False)
    assert scalping.calls == 0
    assert router.scalp_seen is None


def test_scalping_called_when_enabled():
    scalping, router = _run_tick(enable_scalping=True)
    assert scalping.calls == 1


class _FakeRiskManager:
    def calcular_tamano_posicion(self, conviction, entry, sl, equity):
        return 100.0


def test_real_decide_signal_converts_to_unified_signal():
    price = 77850.0
    payload = {
        "htf_ready": True,
        "snapshot": {
            "htf": {"poc": 78691.0, "vah": 79600.0, "val": 77834.0},
            "ltf_1m": [{
                "t": 1, "c": price, "delta": -5.0, "vol": 10.0, "imb": [], "abs": True,
                "iceberg": False, "div": False, "stop_run": None, "initiative_pullback": None,
                "breakout_vol": None, "liquidity_zone": None,
            }],
        },
    }
    snap = SimpleNamespace(scalping_payload=payload, equity_usdt=200.0)
    signal = ScalpingExpert(risk_manager=_FakeRiskManager()).analyze(snap)
    assert signal is not None
    assert signal.strategy_type == "scalping"
    assert signal.pattern_name.value == "ABSORPTION"
    assert signal.direction == "LONG"
    assert signal.sl_price < signal.entry_price < signal.tp_price
