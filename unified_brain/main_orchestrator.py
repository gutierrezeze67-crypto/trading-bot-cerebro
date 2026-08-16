"""Entry point de produccion: corre el loop de mercado + la API WS juntos."""
from __future__ import annotations

import asyncio
import datetime as dt
import os
from pathlib import Path

import structlog
import uvicorn
from dotenv import load_dotenv

# Carga unified_brain/.env (MT5_LOGIN, MT5_PASSWORD, MT5_SERVER, etc.) ANTES
# de que nada mas abajo lea os.environ - sin esto, _build_mcp_config_from_env()
# falla con "Faltan MT5_LOGIN..." aunque el .env exista, porque nunca se inyecta
# al entorno del proceso.
load_dotenv(Path(__file__).resolve().parent / ".env")

from src.execution.mcp_dispatcher import MCPDispatcher, MCPDispatcherConfig, MCPTransport
from src.execution.mt5_direct import MT5DirectExecutor
from src.experts.scalping_expert import ScalpingExpert
from src.experts.swing_expert import SwingExpert
from src.risk.risk_engine import RiskEngine
from src.risk.risk_manager import RiskManager
from src.router.deterministic_router import DeterministicRouter, RouterContext
from src.schemas.market import MarketSnapshot
from src.schemas.risk import AccountState
from src.services.signal_orchestrator import SignalOrchestrator
from src.snapshot_engine import SnapshotEngine

from api import app, manager

logger = structlog.get_logger(__name__)

ZONE_PROXIMITY_PCT = 0.0010


def build_router_context(snapshot: MarketSnapshot) -> RouterContext:
    hour_utc = dt.datetime.utcfromtimestamp(snapshot.ts_ms / 1000).hour if snapshot.ts_ms else dt.datetime.now(dt.timezone.utc).hour

    close = snapshot.htf_vela.get("close")
    near_key_level = False
    if close:
        for level_key in ("poc", "vah", "val"):
            level = snapshot.htf_context.get(level_key)
            if level and abs(close - level) / close <= ZONE_PROXIMITY_PCT:
                near_key_level = True
                break

    return RouterContext(hour_utc=hour_utc, near_key_level=near_key_level)


DEFAULT_MT5_TERMINAL_PATH = r"C:\Program Files\MetaTrader 5\terminal64.exe"


def _build_mcp_config_from_env() -> MCPDispatcherConfig:
    login = os.environ.get("MT5_LOGIN")
    password = os.environ.get("MT5_PASSWORD")
    server = os.environ.get("MT5_SERVER")
    if not login or not password or not server:
        raise RuntimeError(
            "Faltan MT5_LOGIN, MT5_PASSWORD y/o MT5_SERVER en el entorno -- "
            "mcp_dispatcher los necesita para initialize()+login() reales contra MT5 "
            "(ver docstring del modulo). Fallar rapido aca es mejor que arrancar "
            "'sin errores' y quedar reintentando get_account_state() para siempre."
        )

    return MCPDispatcherConfig(
        transport=MCPTransport(os.environ.get("MT5MCP_TRANSPORT", "stdio")),
        url=os.environ.get("MT5MCP_URL"),
        command=os.environ.get("MT5MCP_COMMAND", "mt5mcp"),
        mt5_terminal_path=os.environ.get("MT5_TERMINAL_PATH", DEFAULT_MT5_TERMINAL_PATH),
        mt5_login=int(login),
        mt5_password=password,
        mt5_server=server,
        connect_timeout_s=60.0,
        call_timeout_s=60.0,
    )


def _build_direct_executor_from_env() -> MT5DirectExecutor | None:
    if os.environ.get("MT5_DIRECT_EXECUTION", "").lower() not in ("1", "true", "yes"):
        return None

    login = os.environ.get("MT5_LOGIN")
    password = os.environ.get("MT5_PASSWORD")
    server = os.environ.get("MT5_SERVER")
    if not login or not password or not server:
        raise RuntimeError("MT5_DIRECT_EXECUTION=true requiere MT5_LOGIN, MT5_PASSWORD y MT5_SERVER en el entorno")

    executor = MT5DirectExecutor(
        login=int(login),
        password=password,
        server=server,
        path=os.environ.get("MT5_TERMINAL_PATH", DEFAULT_MT5_TERMINAL_PATH),
    )
    if not executor.connect(symbol=os.environ.get("MT5_SYMBOL", "BTCUSD")):
        raise RuntimeError("MT5DirectExecutor.connect() fallo -- ver logs 'mt5_direct_*' arriba para el detalle")
    return executor


async def _get_account_state(dispatcher: MCPDispatcher, direct_executor: MT5DirectExecutor | None, user_id: str) -> AccountState:
    """Intenta el camino MCP (mcp_dispatcher, via subproceso externo); si no
    esta disponible (subproceso no instalado, no conecto, etc.) cae a
    MT5DirectExecutor.get_account_info(), que ya tiene su propia conexion
    directa al terminal MT5 y no depende de nada externo.

    equity_start_of_day es una aproximacion (= balance actual) en el camino
    directo -- MT5 account_info() no expone la equity de apertura del dia;
    el camino MCP real (get_account_state) si la deriva de history_deals_get.
    """
    try:
        return await dispatcher.get_account_state(user_id)
    except Exception as exc:
        if direct_executor is None:
            raise
        logger.debug("mcp_account_state_fallback_to_direct", error=str(exc))
        info = direct_executor.get_account_info()
        if info is None:
            raise RuntimeError("MT5DirectExecutor.get_account_info() devolvio None (sin conexion)") from exc
        positions = direct_executor.get_positions()
        return AccountState(
            equity=info["equity"],
            equity_start_of_day=info["balance"],
            open_positions=len(positions),
            user_id=user_id,
            balance=info["balance"],
            free_margin=info["free_margin"],
            margin_level=info.get("margin_level"),
        )


def build_default_orchestrator(symbol: str = "BTCUSDT", user_id: str = "default") -> tuple[SignalOrchestrator, SnapshotEngine, MCPDispatcher]:
    engine = SnapshotEngine(symbol=symbol)

    risk_manager = RiskManager(firestore_client=None, capital_inicial=50_000.0)

    scalping_expert = ScalpingExpert(risk_manager=risk_manager)
    swing_expert = SwingExpert()
    router = DeterministicRouter()
    risk_engine = RiskEngine()

    dispatcher = MCPDispatcher(_build_mcp_config_from_env())
    direct_executor = _build_direct_executor_from_env()
    if direct_executor is not None:
        logger.info("mt5_direct_execution_enabled", note="ordenes via mt5_direct.py, lecturas via mcp_dispatcher")

    orchestrator = SignalOrchestrator(
        scalping_expert=scalping_expert,
        swing_expert=swing_expert,
        router=router,
        risk_engine=risk_engine,
        dispatcher=dispatcher,
        account_state_provider=lambda: _get_account_state(dispatcher, direct_executor, user_id),
        emit=lambda event_type, data: manager.broadcast(user_id, event_type, data),
        build_router_context=build_router_context,
        user_id=user_id,
        direct_executor=direct_executor,
        risk_overrides_provider=lambda: manager.get_risk_overrides(user_id),
    )
    return orchestrator, engine, dispatcher


async def run_market_loop(
    engine: SnapshotEngine, orchestrator: SignalOrchestrator, dispatcher: MCPDispatcher, user_id: str, interval_s: float = 5.0
) -> None:
    await engine.start()
    try:
        while True:
            if manager.is_paused(user_id):
                await asyncio.sleep(interval_s)
                continue
            if engine.htf_listo():
                try:
                    equity = (await _get_account_state(dispatcher, orchestrator.direct_executor, user_id)).equity
                except Exception as exc:
                    logger.warning("account_state_unavailable", error=str(exc))
                    await asyncio.sleep(interval_s)
                    continue
                snapshot = MarketSnapshot.from_snapshot_engine(engine, equity_usdt=equity)
                await orchestrator.process_market_tick(snapshot)
            else:
                logger.info("htf_warmup", klines=len(engine.klines_1m))
            await asyncio.sleep(interval_s)
    finally:
        await engine.stop()


async def reconcile_trade_status_loop(direct_executor: MT5DirectExecutor | None, user_id: str, interval_s: float = 30.0) -> None:
    """signal_orchestrator.py emite trade_executed con status='OPEN'/pnl=None
    fijo a proposito (no hace polling, ver comentario ahi) -- sin esto
    /api/history miente para siempre que un trade sigue abierto aunque ya
    haya cerrado por SL/TP hace dias. Este loop background confirma contra
    MT5 real cada trade que el historial en memoria todavia marca OPEN, y
    corrige status/pnl con update_trade_status() cuando corresponde.

    Solo lee (positions_get/history_deals_get via get_position_status) -- no
    manda ninguna orden, no puede afectar la operativa real."""
    if direct_executor is None:
        logger.info("reconcile_trade_status_disabled", reason="direct_executor no configurado (MT5_DIRECT_EXECUTION!=true)")
        return
    while True:
        await asyncio.sleep(interval_s)
        try:
            for ticket in manager.get_open_tickets(user_id):
                result = await asyncio.to_thread(direct_executor.get_position_status, ticket)
                if result["status"] != "UNKNOWN":
                    updated = manager.update_trade_status(user_id, ticket, result["status"], result["pnl"])
                    if updated and result["status"] == "CLOSED":
                        logger.info("trade_status_reconciled", ticket=ticket, pnl=result["pnl"])
        except Exception as exc:  # noqa: BLE001 - un ciclo roto no puede tirar el loop entero
            logger.warning("reconcile_trade_status_failed", error=str(exc))


async def main() -> None:
    user_id = os.environ.get("UNIFIED_BRAIN_USER_ID", "default")
    orchestrator, engine, dispatcher = build_default_orchestrator(symbol=os.environ.get("UNIFIED_BRAIN_SYMBOL", "BTCUSDT"), user_id=user_id)

    try:
        await dispatcher.connect(user_id)
    except (Exception, asyncio.CancelledError) as exc:
        if orchestrator.direct_executor is None:
            logger.error("mcp_connect_failed", user_id=user_id, error=str(exc))
            raise
        logger.warning(
            "mcp_connect_failed_using_direct_fallback", user_id=user_id, error=str(exc),
            note="mcp_dispatcher (subproceso externo) no disponible -- se sigue con MT5DirectExecutor para lecturas y ordenes",
        )

    port = int(os.environ.get("UNIFIED_BRAIN_PORT", "8001"))
    server = uvicorn.Server(uvicorn.Config(app, host="0.0.0.0", port=port, log_level="info"))

    try:
        await asyncio.gather(
            run_market_loop(engine, orchestrator, dispatcher, user_id),
            reconcile_trade_status_loop(orchestrator.direct_executor, user_id),
            server.serve(),
        )
    finally:
        await dispatcher.disconnect_all()
        if orchestrator.direct_executor is not None:
            await asyncio.to_thread(orchestrator.direct_executor.disconnect)


if __name__ == "__main__":
    asyncio.run(main())