"""Director de orquesta: MarketSnapshot -> expertos -> router -> risk -> mcp
dispatch -> log de trade -> eventos WS.

No hay hoy un backend de trading al que enchufarse (orderflow_backend/ es
motor de datos de mercado puro -- footprint/velas/volumen, sin ordenes ni
riesgo, ver unified_brain/api.py para el detalle). Por eso el emisor de
eventos es un callback inyectado (`emit`), no un import directo a un server
especifico -- unified_brain/api.py trae un ConnectionManager cuyo
`broadcast` calza esa firma.

Persistencia de trades: stub minimo a JSONL en results/ (mismo directorio
que ya usa el resto del repo para resultados de backtest). No se conecta a
Firestore aca a proposito -- src/risk_manager.py ya tiene su propio cliente
Firestore para equity state, y mezclarlos hoy es mas acoplamiento del que
pidio esta tarea. Reemplazar _persist_trade por una escritura real a DB
cuando haga falta auditar en produccion.
"""
from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Protocol

import structlog

if TYPE_CHECKING:
    from src.services.sensei_zone_generator import SenseiZoneGenerator

from src.execution.mcp_dispatcher import KNOWN_ERROR_HINTS, ExecutionResult, MCPDispatcher
from src.execution.mt5_direct import MT5DirectExecutor, OrderResult as DirectOrderResult
from src.experts.base_expert import BaseExpert
from src.router.deterministic_router import DeterministicRouter, RouterContext, RouterDecision
from src.risk.risk_engine import RiskEngine
from src.schemas.market import MarketSnapshot
from src.schemas.risk import AccountState, RiskConfig
from src.schemas.signals import UnifiedSignal
from src.services.zone_cache import ZoneCache

logger = structlog.get_logger(__name__)

EmitFn = Callable[[str, dict[str, Any]], Awaitable[None]]

RESULTS_DIR = Path(__file__).resolve().parents[2] / "results"
TRADE_LOG_PATH = RESULTS_DIR / "unified_brain_trades.jsonl"


class AccountStateProvider(Protocol):
    async def __call__(self) -> AccountState: ...


def _persist_trade(signal: UnifiedSignal, decision: RouterDecision, exec_result: ExecutionResult, snapshot: MarketSnapshot) -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    record = {
        "ts_ms": snapshot.ts_ms,
        "signal": signal.model_dump(mode="json"),
        "router_action": decision.action,
        "router_detail": decision.detail,
        "execution": exec_result.model_dump(mode="json"),
    }
    with TRADE_LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def _direct_result_to_execution_result(result: DirectOrderResult, signal: UnifiedSignal) -> ExecutionResult:
    """MT5DirectExecutor.OrderResult -> el mismo ExecutionResult que devuelve
    MCPDispatcher.execute_order, para que process_market_tick no tenga que
    saber cual de los dos caminos se uso. sl/tp no vienen en la respuesta de
    MT5 (OrderResult no los trae) -- se usan los que la señal ya pedia, no
    hay otro valor mas correcto para mostrar."""
    if result.success:
        return ExecutionResult(
            ok=True, ticket=result.ticket, volume=result.volume, price=result.price,
            sl=signal.sl_price, tp=signal.tp_price, latency_ms=result.latency_ms,
            raw={"retcode": result.retcode, "comment": result.comment},
        )
    error_code = next((code for hint, code in KNOWN_ERROR_HINTS.items() if hint in (result.error or "").lower()), "UNKNOWN")
    return ExecutionResult(ok=False, latency_ms=result.latency_ms, error=result.error, error_code=error_code)


class _AccountCache:
    """Cache de 5s sobre el provider real, para no golpear MT5/Firestore en cada tick."""

    def __init__(self, provider: AccountStateProvider, ttl_s: float = 5.0) -> None:
        self._provider = provider
        self._ttl_s = ttl_s
        self._cached: AccountState | None = None
        self._cached_at = 0.0
        self._lock = asyncio.Lock()

    async def get(self) -> AccountState:
        async with self._lock:
            now = time.monotonic()
            if self._cached is None or (now - self._cached_at) > self._ttl_s:
                self._cached = await self._provider()
                self._cached_at = now
            return self._cached


class SignalOrchestrator:
    def __init__(
        self,
        scalping_expert: BaseExpert,
        swing_expert: BaseExpert,
        router: DeterministicRouter,
        risk_engine: RiskEngine,
        dispatcher: MCPDispatcher,
        account_state_provider: AccountStateProvider,
        emit: EmitFn,
        build_router_context: Callable[[MarketSnapshot], RouterContext],
        user_id: str = "default",
        on_trade_executed: Callable[[ExecutionResult], None] | None = None,
        direct_executor: MT5DirectExecutor | None = None,
        risk_overrides_provider: Callable[[], dict[str, float]] | None = None,
        zone_cache: ZoneCache | None = None,
        sensei_generator: "SenseiZoneGenerator | None" = None,
    ) -> None:
        self.scalping_expert = scalping_expert
        self.swing_expert = swing_expert
        self.router = router
        self.risk_engine = risk_engine
        self.dispatcher = dispatcher
        self._account_cache = _AccountCache(account_state_provider)
        self.emit = emit
        self.build_router_context = build_router_context
        self.user_id = user_id
        self.on_trade_executed = on_trade_executed
        # Si esta seteado, execute_order() pasa por aca en vez de por
        # dispatcher (MCP) -- ver unified_brain/execution/mt5_direct.py.
        # mt5.order_send() es sincrono/bloqueante, SIEMPRE via
        # asyncio.to_thread (si no, congela el loop entero, API WS incluida).
        self.direct_executor = direct_executor
        # Overrides en vivo (risk_pct_per_trade/max_trades_per_day/
        # max_daily_loss_pct) via unified_brain/api.py:cmd=config -- reales,
        # no cosmeticos: se aplican en CADA tick sobre self.risk_engine.config
        # (que nunca se muta, se copia). Ver _effective_risk_config().
        self.risk_overrides_provider = risk_overrides_provider
        # Cache de zonas Sensei -- NADA la puebla ni la consulta todavia
        # (ni SwingExpert.analyze() ni ningun generador real existen aun).
        # Instanciada aca solo para tener el ciclo de vida (cleanup_old por
        # tick) listo antes de conectar la logica real -- ver
        # src/schemas/sensei_zones.py y src/services/zone_cache.py.
        self.zone_cache = zone_cache if zone_cache is not None else ZoneCache()
        # Generador Sensei -- opcional, None por default (nadie lo instancia
        # todavia en build_default_orchestrator). Si esta seteado, se llama
        # on_bar_close() por cada TF que cerro en el tick (snapshot.closed_timeframes),
        # ANTES de swing_expert.analyze() -- swing_expert.analyze() sigue sin
        # consultar zone_cache (confluence_enabled sigue en false), asi que
        # esto no cambia ningun resultado de decision todavia, solo puebla la
        # cache para cuando se conecte de verdad.
        self.sensei_generator = sensei_generator

    def _effective_risk_config(self) -> RiskConfig:
        if self.risk_overrides_provider is None:
            return self.risk_engine.config
        overrides = self.risk_overrides_provider()
        if not overrides:
            return self.risk_engine.config
        return self.risk_engine.config.model_copy(update=overrides)

    def _risk_update_payload(self, account: AccountState, risk_config: RiskConfig, status: str, code: str | None = None, trades_today_delta: int = 0) -> dict[str, Any]:
        return {
            "status": status,
            "code": code,
            "trades_today": account.trades_today + trades_today_delta,
            "max_trades_day": risk_config.max_trades_per_day,
            "daily_pnl_pct": account.daily_pnl_pct,
            "max_daily_loss_pct": risk_config.max_daily_loss_pct,
            "free_margin": account.free_margin,
            "margin_level": account.margin_level,
            "balance": account.balance,
            "equity": account.equity,
            "equity_start_of_day": account.equity_start_of_day,
        }

    async def process_market_tick(self, snapshot: MarketSnapshot) -> None:
        log = logger.bind(ts_ms=snapshot.ts_ms, symbol=snapshot.symbol)
        try:
            self.zone_cache.cleanup_old(snapshot.ts_ms)

            if self.sensei_generator is not None:
                for tf_min in snapshot.closed_timeframes:
                    bar = snapshot.get_last_closed_bar(tf_min)
                    if bar is not None:
                        self.sensei_generator.on_bar_close(snapshot.symbol, tf_min, bar)

            account = await self._account_cache.get()

            risk_config = self._effective_risk_config()
            risk_engine = self.risk_engine if risk_config is self.risk_engine.config else RiskEngine(risk_config)

            # ScalpingExpert desactivado para este deployment: backtest real
            # (walk-forward + out-of-time, ~465 trades) mostro PF 0.30 en
            # cuentas de fondeo (FundedNext/FundingPips) -- los costos lo
            # destruyen -- mientras que SwingExpert solo sostiene PF 2.65-3.00,
            # y combinar ambos en el router diluye el resultado a PF 2.03. No
            # se toca ScalpingExpert.analyze() ni el Router: scalping
            # simplemente no le propone candidatos al router en este deploy.
            scalp_signal = None
            swing_signal = await asyncio.to_thread(self.swing_expert.analyze, snapshot)

            context = self.build_router_context(snapshot)
            decision = self.router.route(account, scalp_signal, swing_signal, context)

            if decision.action == "hold":
                detail = decision.detail
                if decision.hold_reason == "NO_PATTERN":
                    # decision.detail es siempre el generico "ningun experto
                    # encontro setup" -- swing_expert.brain.last_reject_reason
                    # (canal de diagnostico aparte, ver brain_htf_funding.py)
                    # tiene el motivo real (compresion de ATR, sin patron,
                    # cooldown de zona, RR neto bajo, etc). getattr con
                    # default None: ScalpingExpert no tiene atributo .brain,
                    # no puede romper si algun dia vuelve a estar activo.
                    swing_reason = getattr(getattr(self.swing_expert, "brain", None), "last_reject_reason", None)
                    if swing_reason and swing_reason not in ("OK", "SIN_EVALUAR"):
                        detail = f"{decision.detail} (swing: {swing_reason})"
                log.info("router_hold", reason=decision.hold_reason, detail=detail)
                await self.emit("advisor_update", {"text": detail, "reasoning": detail, "type": "hold", "pattern": None, "hold_reason": decision.hold_reason})
                await self.emit("risk_update", self._risk_update_payload(account, risk_config, status="OK"))
                return

            assert decision.signal is not None
            signal = decision.signal

            risk_result = risk_engine.pre_flight(account, signal)
            if not risk_result.allowed or risk_result.adjusted_lot is None:
                code = risk_result.code if not risk_result.allowed else "INVALID_SIGNAL"
                msg = risk_result.msg if not risk_result.allowed else "risk_engine no devolvio adjusted_lot pese a allowed=True"
                log.warning("signal_blocked", code=code, msg=msg)
                await self.emit("signal_blocked", {"signal_id": signal.signal_id, "code": code, "msg": msg})
                await self.emit("risk_update", self._risk_update_payload(account, risk_config, status="BLOCKED", code=code))
                await self.emit("advisor_update", {"text": msg, "reasoning": msg, "type": "risk_block", "pattern": signal.pattern_name.value, "hold_reason": code})
                return

            if self.direct_executor is not None:
                direct_result = await asyncio.to_thread(
                    self.direct_executor.execute_order,
                    symbol=signal.metadata.get("mt5_symbol") or os.environ.get("MT5_SYMBOL", "BTCUSD"),
                    side="BUY" if signal.direction == "LONG" else "SELL",
                    volume=risk_result.adjusted_lot,
                    sl=signal.sl_price,
                    tp=signal.tp_price,
                    comment=f"UB:{signal.strategy_type[:4]}:{signal.pattern_name.value[:12]}"[:31],
                )
                exec_result = _direct_result_to_execution_result(direct_result, signal)
            else:
                exec_result = await self.dispatcher.execute_order(self.user_id, signal, lot=risk_result.adjusted_lot)

            if not exec_result.ok:
                log.error("execution_failed", error=exec_result.error, error_code=exec_result.error_code)
                await self.emit("signal_blocked", {"signal_id": signal.signal_id, "code": exec_result.error_code or "EXECUTION_ERROR", "msg": exec_result.error or ""})
                await self.emit("risk_update", self._risk_update_payload(account, risk_config, status="ERROR"))
                await self.emit("advisor_update", {"text": exec_result.error or "", "reasoning": exec_result.error or "", "type": "error", "pattern": signal.pattern_name.value, "hold_reason": exec_result.error_code})
                return

            _persist_trade(signal, decision, exec_result, snapshot)
            if self.on_trade_executed is not None:
                self.on_trade_executed(exec_result)

            await self.emit("trade_executed", {
                "signal_id": signal.signal_id,
                "strategy_type": signal.strategy_type,
                "pattern_name": signal.pattern_name.value,
                "pattern": signal.pattern_name.value,
                "direction": signal.direction,
                "side": "BUY" if signal.direction == "LONG" else "SELL",
                "symbol": signal.metadata.get("mt5_symbol"),
                "ticket": exec_result.ticket,
                "volume": exec_result.volume,
                "price": exec_result.price,
                "sl": exec_result.sl,
                "tp": exec_result.tp,
                "latency_ms": exec_result.latency_ms,
                "pattern_wr": signal.winrate_historical,
                "confidence": signal.confidence,
                "target_rr": signal.target_rr,
                "reasoning": signal.reasoning,
                # pnl/status: NO hay tracking de cierre de posicion todavia
                # (requeriria polling positions_get/history_deals_get por
                # ticket, no implementado) -- status queda fijo en "OPEN" y
                # pnl en None en vez de inventar un valor. El frontend no
                # deberia mostrar esto como "cerrado" ni calcular ganancias.
                "pnl": None,
                "status": "OPEN",
            })
            await self.emit("risk_update", self._risk_update_payload(account, risk_config, status="OK", trades_today_delta=1))
            await self.emit("advisor_update", {"text": signal.reasoning, "reasoning": signal.reasoning, "type": "signal", "pattern": signal.pattern_name.value, "hold_reason": None})

        except Exception as exc:  # noqa: BLE001 - un tick roto no puede tirar el loop entero
            log.exception("process_market_tick_failed", error=str(exc))
            await self.emit("error", {"msg": str(exc), "message": str(exc)})
