"""Cliente MCP real -> mt5mcp (SDK oficial `mcp`, no MetaTrader5 directo).

CONTRATO VERIFICADO CONTRA EL PAQUETE REAL INSTALADO (mcp-metatrader5-server
0.1.8, `pip show -f mcp-metatrader5-server`, codigo fuente leido completo en
site-packages/mcp_mt5/main.py) -- ya NO es un supuesto sin verificar:

- Server: entry point `mt5mcp` (console_scripts, ver entry_points.txt) o
  `python -m mcp_mt5`. Transporte default STDIO (env MT5_MCP_TRANSPORT=http
  para HTTP). Es un FastMCP, no acepta --login/--password/--server como
  argumentos de arranque -- esos son ARGUMENTOS DE TOOLS que se llaman
  DESPUES de conectar, en este orden obligatorio:
    1. initialize(path=<ruta a terminal64.exe>)
    2. login(login=<int>, password=<str>, server=<str>)  [opcional si la
       terminal ya esta logueada]
    3. recien ahi cualquier otra tool (get_account_info, order_send, etc.)
  connect() hace los 3 pasos automaticamente si mt5_terminal_path esta
  seteado en el config.

- NO EXISTE una tool "place_risk_order" (no es un wrapper de riesgo, es un
  server generico de la API de MT5). La tool real para operar es
  order_send(request: OrderRequest) -> OrderResult, con action=1
  (TRADE_ACTION_DEAL), type=0 (BUY) / 1 (SELL), volume en LOTES, price
  obligatorio (se pide con get_symbol_info_tick antes). El pips/rr/risk_pct
  que armaba la version anterior de este archivo no aplica -- RiskEngine ya
  entrega el lote (adjusted_lot) directo.

- Nombres reales: get_account_info (no "account_info"), positions_get,
  history_deals_get, get_symbol_info_tick (no "symbol_info_tick").

Conexiones persistentes por user_id: get_account_state se llama cada pocos
segundos desde el orchestrator -- reabrir el handshake MCP (+ initialize +
login) en cada tick seria carisimo. connect(user_id) hace todo una vez y
queda vivo; execute_order/get_account_state reusan la sesion.
disconnect_all() hay que llamarlo al apagar el proceso.

ADVERTENCIA PROBADA EN LOCAL: contra un puerto donde NO hay absolutamente
nada escuchando, connect() puede tardar unos segundos en fallar (bounded,
ya no cuelga indefinidamente) y de paso imprimir ruido de stderr del SDK
(limpieza de generadores async durante la cancelacion) -- cosmetico, no
afecta el resultado. Quien llame a connect() debe capturar
`(Exception, asyncio.CancelledError)`, no solo `Exception` (CancelledError
no hereda de Exception desde Python 3.8).
"""
from __future__ import annotations

import asyncio
import datetime as dt
import json
import time
from contextlib import AsyncExitStack
from enum import Enum
from typing import Any

import structlog
from mcp import ClientSession
from mcp.client.sse import sse_client
from mcp.client.stdio import StdioServerParameters, stdio_client
from mcp.client.streamable_http import streamablehttp_client
from pydantic import BaseModel, Field

from src.schemas.risk import AccountState
from src.schemas.signals import UnifiedSignal

logger = structlog.get_logger(__name__)

INITIALIZE_TOOL = "initialize"
LOGIN_TOOL = "login"
ORDER_SEND_TOOL = "order_send"
ACCOUNT_INFO_TOOL = "get_account_info"
POSITIONS_GET_TOOL = "positions_get"
HISTORY_DEALS_GET_TOOL = "history_deals_get"
SYMBOL_INFO_TICK_TOOL = "get_symbol_info_tick"

# MT5 (paquete MetaTrader5): TRADE_ACTION_DEAL=1, ORDER_TYPE_BUY=0, ORDER_TYPE_SELL=1.
TRADE_ACTION_DEAL = 1
ORDER_TYPE_BUY = 0
ORDER_TYPE_SELL = 1

# MT5 Deal.entry: DEAL_ENTRY_IN=0 (abre posicion). DEAL_ENTRY_OUT=1,
# DEAL_ENTRY_INOUT=2, DEAL_ENTRY_OUT_BY=3 no cuentan como "trade nuevo".
DEAL_ENTRY_IN = 0

# Substrings de los mensajes de error reales que arma mcp_mt5/main.py:order_send
# (retcode_messages) -- confirmados contra el codigo fuente, no adivinados.
KNOWN_ERROR_HINTS = {
    "requote": "REQUOTE",
    "no money": "NO_MONEY",
    "not enough money": "NO_MONEY",
    "market closed": "MARKET_CLOSED",
    "invalid stops": "INVALID_STOPS",
    "trade disabled": "TRADE_DISABLED",
    "autotrading disabled": "TRADE_DISABLED",
    "invalid volume": "INVALID_VOLUME",
    "invalid price": "INVALID_PRICE",
}


class MCPTransport(str, Enum):
    STDIO = "stdio"
    SSE = "sse"
    STREAMABLE_HTTP = "streamable_http"


class MCPDispatcherConfig(BaseModel):
    transport: MCPTransport = MCPTransport.STDIO
    url: str | None = Field(default=None, description="requerido para sse/streamable_http, ej. tunel Cloudflare al VPS")
    command: str | None = Field(default=None, description="requerido para stdio, ej. 'mt5mcp' o el path al exe")
    args: list[str] = Field(default_factory=list)
    call_timeout_s: float = Field(gt=0, default=15.0)
    connect_timeout_s: float = Field(gt=0, default=10.0)
    max_retries: int = Field(ge=0, default=2)
    retry_backoff_s: float = Field(gt=0, default=1.5)
    max_spread_pips: float | None = Field(default=None, description="si se setea, execute_order valida get_symbol_info_tick antes de disparar y bloquea con SPREAD_WIDE si se excede")

    mt5_terminal_path: str | None = Field(default=None, description="path a terminal64.exe -- connect() llama initialize(path=...) si esta seteado")
    mt5_login: int | None = None
    mt5_password: str | None = None
    mt5_server: str | None = None


class ExecutionResult(BaseModel):
    ok: bool
    ticket: int | None = None
    volume: float | None = None
    price: float | None = None
    sl: float | None = None
    tp: float | None = None
    latency_ms: float
    raw: dict[str, Any] | None = None
    error: str | None = None
    error_code: str | None = None


class MCPDispatcherError(RuntimeError):
    pass


class MCPDispatcher:
    def __init__(self, config: MCPDispatcherConfig) -> None:
        self.config = config
        if config.transport == MCPTransport.STDIO and not config.command:
            raise MCPDispatcherError("transport=stdio requiere config.command")
        if config.transport != MCPTransport.STDIO and not config.url:
            raise MCPDispatcherError("transport=sse/streamable_http requiere config.url")

        self._sessions: dict[str, ClientSession] = {}
        self._stacks: dict[str, AsyncExitStack] = {}
        self._connect_locks: dict[str, asyncio.Lock] = {}

    # ------------------------------------------------------------------
    # Ciclo de vida de la conexion
    # ------------------------------------------------------------------

    async def connect(self, user_id: str) -> None:
        if user_id in self._sessions:
            return
        lock = self._connect_locks.setdefault(user_id, asyncio.Lock())
        async with lock:
            if user_id in self._sessions:  # otra corutina pudo conectar mientras esperabamos
                return

            stack = AsyncExitStack()
            try:
                # OJO: el timeout de conexion/lectura se pasa a los clientes
                # de transporte (sse_client/streamablehttp_client) y a
                # ClientSession(read_timeout_seconds=...), NUNCA envolviendo
                # estas llamadas en asyncio.wait_for() -- wait_for corre el
                # awaitable en un Task nuevo, y como stdio_client/
                # streamablehttp_client abren su propio task group (anyio)
                # dentro del task ACTUAL, cancelar desde un Task distinto
                # rompe el cancel scope ("Attempted to exit cancel scope in
                # a different task than it was entered in") y tapa el error
                # real (ej. connection refused) con un crash de anyio. Visto
                # en pruebas locales contra un puerto sin nada escuchando.
                if self.config.transport == MCPTransport.STDIO:
                    params = StdioServerParameters(command=self.config.command, args=self.config.args)
                    read, write = await stack.enter_async_context(stdio_client(params))
                elif self.config.transport == MCPTransport.SSE:
                    read, write = await stack.enter_async_context(sse_client(self.config.url, timeout=self.config.connect_timeout_s))
                else:
                    read, write, _get_session_id = await stack.enter_async_context(
                        streamablehttp_client(self.config.url, timeout=self.config.connect_timeout_s)
                    )

                session = await stack.enter_async_context(
                    ClientSession(read, write, read_timeout_seconds=dt.timedelta(seconds=self.config.call_timeout_s))
                )
                await session.initialize()

                # Secuencia obligatoria del server real antes de que
                # cualquier otra tool funcione (ver docstring del modulo).
                if self.config.mt5_terminal_path:
                    init_result = await session.call_tool(INITIALIZE_TOOL, {"path": self.config.mt5_terminal_path})
                    if not self._parse_json_content(init_result):
                        raise MCPDispatcherError(f"initialize(path={self.config.mt5_terminal_path!r}) devolvio False")

                    if self.config.mt5_login is not None:
                        login_result = await session.call_tool(
                            LOGIN_TOOL,
                            {"login": self.config.mt5_login, "password": self.config.mt5_password, "server": self.config.mt5_server},
                        )
                        if not self._parse_json_content(login_result):
                            raise MCPDispatcherError(f"login(login={self.config.mt5_login}, server={self.config.mt5_server!r}) devolvio False")
            except Exception:
                await stack.aclose()
                raise

            self._sessions[user_id] = session
            self._stacks[user_id] = stack
            logger.info("mcp_connected", user_id=user_id, transport=self.config.transport.value)

    async def disconnect(self, user_id: str) -> None:
        stack = self._stacks.pop(user_id, None)
        self._sessions.pop(user_id, None)
        if stack is not None:
            await stack.aclose()
            logger.info("mcp_disconnected", user_id=user_id)

    async def disconnect_all(self) -> None:
        for user_id in list(self._sessions.keys()):
            await self.disconnect(user_id)

    def _require_session(self, user_id: str) -> ClientSession:
        session = self._sessions.get(user_id)
        if session is None:
            raise MCPDispatcherError(f"MCP no conectado para user_id={user_id!r} -- llamar await dispatcher.connect(user_id) primero")
        return session

    # ------------------------------------------------------------------
    # Helpers de parseo / simbolo
    # ------------------------------------------------------------------

    def _pip_size(self, symbol: str) -> float:
        """Usa el tick_size REAL y validado de ASSET_CONFIG cuando existe
        (BTCUSDT: 0.10 -- ver strategies/htf_funding_btc/config/assets.py).
        Solo cae al heuristico generico para simbolos sin config validada
        (XAUUSD/US30/etc -- los mismos que FUNDEDNEXT_PARAMS.md ya marca
        como "sin backtest, no operar" hasta tener costos reales)."""
        try:
            from strategies.htf_funding_btc.config.assets import get_asset_config

            return float(get_asset_config(symbol)["tick_size"])
        except (ImportError, ValueError, KeyError):
            pass

        s = symbol.upper()
        if "JPY" in s:
            return 0.01
        if "BTC" in s or "ETH" in s:
            return 0.01  # fallback generico, NO el valor validado de BTCUSDT (eso sale de ASSET_CONFIG arriba)
        return 0.0001

    @staticmethod
    def _parse_json_content(result: Any) -> Any:
        """Confirmado contra el server real (FastMCP 3.4.2): las tools que
        devuelven list[...] (positions_get, history_deals_get) NO llenan
        `content` cuando la lista viene vacia -- ponen el resultado en
        `structuredContent` como {"result": [...]}. Las que devuelven un
        BaseModel (get_account_info) SI llenan `content` con texto JSON Y
        tambien structuredContent, sin el wrapper "result". Por eso
        structuredContent es la fuente primaria (mas confiable, nunca viene
        vacia por una lista vacia); content-como-texto es el fallback."""
        if getattr(result, "isError", False):
            text_parts = [c.text for c in getattr(result, "content", []) if getattr(c, "type", None) == "text"]
            error_msg = text_parts[0] if text_parts else "error sin detalle"
            raise MCPDispatcherError(f"tool error: {error_msg}")

        structured = getattr(result, "structuredContent", None)
        if structured is not None:
            if isinstance(structured, dict) and set(structured.keys()) == {"result"}:
                return structured["result"]
            return structured

        text_parts = [c.text for c in getattr(result, "content", []) if getattr(c, "type", None) == "text"]
        if not text_parts:
            raise MCPDispatcherError("respuesta MCP sin content ni structuredContent parseable")
        try:
            return json.loads(text_parts[0])
        except (json.JSONDecodeError, TypeError) as exc:
            raise MCPDispatcherError(f"respuesta MCP no es JSON valido: {text_parts[0]!r}") from exc

    def _parse_execution_result(self, result: Any, latency_ms: float) -> ExecutionResult:
        """Parsea un OrderResult real: {retcode, deal, order, volume, price,
        bid, ask, comment, request_id, retcode_external, request}. No hay
        campo "ticket" -- se usa "order" (el ticket de la orden/posicion
        resultante). sl/tp no vienen en la respuesta, se leen del "request"
        que el server hace eco (son los que nosotros mandamos).

        isError=True (rechazo de MT5/broker) es siempre texto plano, no
        structuredContent -- el server real tira un ValueError con el
        mensaje de retcode, ver order_send en mcp_mt5/main.py. Para el caso
        de exito preferimos structuredContent, mismo criterio que
        _parse_json_content (ver ahi el porque)."""
        if bool(getattr(result, "isError", False)):
            text_parts = [c.text for c in getattr(result, "content", []) if getattr(c, "type", None) == "text"]
            error_msg = text_parts[0] if text_parts else "error sin detalle"
            error_code = next((code for hint, code in KNOWN_ERROR_HINTS.items() if hint in error_msg.lower()), "UNKNOWN")
            return ExecutionResult(ok=False, latency_ms=latency_ms, raw={"raw_text": error_msg}, error=error_msg, error_code=error_code)

        raw: dict[str, Any] | None = None
        structured = getattr(result, "structuredContent", None)
        if isinstance(structured, dict):
            raw = structured.get("result") if set(structured.keys()) == {"result"} else structured
        if raw is None:
            text_parts = [c.text for c in getattr(result, "content", []) if getattr(c, "type", None) == "text"]
            if text_parts:
                try:
                    raw = json.loads(text_parts[0])
                except (json.JSONDecodeError, TypeError):
                    raw = {"raw_text": text_parts[0]}

        if raw is None:
            return ExecutionResult(ok=False, latency_ms=latency_ms, error="respuesta sin content ni structuredContent parseable", error_code="UNPARSEABLE")

        echoed_request = raw.get("request") or {}
        return ExecutionResult(
            ok=True,
            ticket=raw.get("order"),
            volume=raw.get("volume"),
            price=raw.get("price"),
            sl=echoed_request.get("sl"),
            tp=echoed_request.get("tp"),
            latency_ms=latency_ms,
            raw=raw,
        )

    # ------------------------------------------------------------------
    # Estado de cuenta real
    # ------------------------------------------------------------------

    async def get_account_state(self, user_id: str) -> AccountState:
        session = self._require_session(user_id)
        log = logger.bind(user_id=user_id)

        call_timeout = dt.timedelta(seconds=self.config.call_timeout_s)

        acc_result = await session.call_tool(ACCOUNT_INFO_TOOL, {}, read_timeout_seconds=call_timeout)
        acc = self._parse_json_content(acc_result)
        if isinstance(acc, list):  # por si el server envuelve la respuesta en una lista de 1
            acc = acc[0] if acc else {}

        pos_result = await session.call_tool(POSITIONS_GET_TOOL, {}, read_timeout_seconds=call_timeout)
        positions = self._parse_json_content(pos_result)
        if not isinstance(positions, list):
            positions = []

        today_utc = dt.datetime.now(dt.timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        deals_result = await session.call_tool(
            HISTORY_DEALS_GET_TOOL, {"from_date": today_utc.isoformat()}, read_timeout_seconds=call_timeout
        )
        deals = self._parse_json_content(deals_result)
        if not isinstance(deals, list):
            deals = []

        realized_pnl_today = 0.0
        trades_today = 0
        for deal in deals:
            realized_pnl_today += (
                float(deal.get("profit") or 0) + float(deal.get("swap") or 0) + float(deal.get("commission") or 0) + float(deal.get("fee") or 0)
            )
            if deal.get("entry") == DEAL_ENTRY_IN:
                trades_today += 1

        balance = float(acc.get("balance", 0.0))
        equity = float(acc.get("equity", balance))
        # Aproximacion: MT5 no expone "equity a las 00:00 UTC" directo.
        # balance actual ya incluye los deals realizados de hoy, asi que
        # restarlos aproxima el balance al inicio del dia. No contempla
        # posiciones que ya estaban abiertas ANTES de medianoche con PnL
        # flotante -- edge case conocido, ver docstring de la clase.
        equity_start_of_day = balance - realized_pnl_today
        if equity_start_of_day <= 0:
            log.warning("equity_start_of_day_invalido", equity_start_of_day=equity_start_of_day, fallback_a=equity)
            equity_start_of_day = equity

        return AccountState(
            user_id=user_id,
            equity=equity,
            equity_start_of_day=equity_start_of_day,
            balance=balance,
            free_margin=float(acc["margin_free"]) if acc.get("margin_free") is not None else None,
            margin_level=float(acc["margin_level"]) if acc.get("margin_level") is not None else None,
            realized_pnl_today=realized_pnl_today,
            open_positions=len(positions),
            trades_today=trades_today,
        )

    # ------------------------------------------------------------------
    # Ejecucion
    # ------------------------------------------------------------------

    def _build_order_request(self, signal: UnifiedSignal, lot: float, price: float) -> dict[str, Any]:
        """Arma el dict que espera order_send(request: OrderRequest) ->
        OrderResult -- action=1 (TRADE_ACTION_DEAL), type 0=BUY/1=SELL,
        volume en LOTES (no USDT, no pips)."""
        return {
            "action": TRADE_ACTION_DEAL,
            "symbol": signal.metadata.get("mt5_symbol", "BTCUSD"),
            "volume": round(lot, 2),
            "type": ORDER_TYPE_BUY if signal.direction == "LONG" else ORDER_TYPE_SELL,
            "price": price,
            "sl": signal.sl_price,
            "tp": signal.tp_price,
            "comment": f"UB:{signal.strategy_type[:4]}:{signal.pattern_name.value[:12]}"[:31],
        }

    async def execute_order(self, user_id: str, signal: UnifiedSignal, lot: float) -> ExecutionResult:
        """lot: tamano en LOTES ya calculado por RiskEngine.pre_flight
        (RiskCheckResult.adjusted_lot) -- order_send no sabe de risk_pct,
        necesita el lote final, no un porcentaje a convertir aca."""
        log = logger.bind(user_id=user_id, signal_id=signal.signal_id, strategy_type=signal.strategy_type, pattern=signal.pattern_name.value)
        session = self._require_session(user_id)
        mt5_symbol = signal.metadata.get("mt5_symbol", "BTCUSD")
        pip_size = self._pip_size(mt5_symbol)
        call_timeout = dt.timedelta(seconds=self.config.call_timeout_s)
        start = time.monotonic()

        # price es OBLIGATORIO para order_send (no hay "market order sin
        # precio" en la API de MT5) -- siempre pedimos el tick actual, no
        # solo cuando hay guard de spread configurado.
        try:
            tick_result = await session.call_tool(SYMBOL_INFO_TICK_TOOL, {"symbol": mt5_symbol}, read_timeout_seconds=call_timeout)
            tick = self._parse_json_content(tick_result)
            bid, ask = float(tick["bid"]), float(tick["ask"])
        except MCPDispatcherError as exc:
            latency_ms = (time.monotonic() - start) * 1000
            log.error("symbol_info_tick_failed", error=str(exc))
            return ExecutionResult(ok=False, latency_ms=latency_ms, error=str(exc), error_code="NO_TICK")

        if self.config.max_spread_pips is not None:
            spread_pips = (ask - bid) / pip_size
            if spread_pips > self.config.max_spread_pips:
                latency_ms = (time.monotonic() - start) * 1000
                log.warning("spread_too_wide", spread_pips=spread_pips, max_spread_pips=self.config.max_spread_pips)
                return ExecutionResult(
                    ok=False, latency_ms=latency_ms,
                    error=f"spread {spread_pips:.1f} pips > max {self.config.max_spread_pips}",
                    error_code="SPREAD_WIDE",
                )

        price = ask if signal.direction == "LONG" else bid
        order_request = self._build_order_request(signal, lot, price)
        last_error: Exception | None = None

        for attempt in range(1, self.config.max_retries + 2):
            try:
                result = await session.call_tool(ORDER_SEND_TOOL, {"request": order_request}, read_timeout_seconds=call_timeout)
                latency_ms = (time.monotonic() - start) * 1000
                parsed = self._parse_execution_result(result, latency_ms)
                if parsed.ok:
                    log.info("mcp_order_executed", ticket=parsed.ticket, latency_ms=latency_ms)
                else:
                    log.warning("mcp_order_rejected", error=parsed.error, error_code=parsed.error_code)
                return parsed
            except Exception as exc:  # noqa: BLE001 - queremos capturar cualquier falla de transporte/protocolo y reintentar
                last_error = exc
                log.warning("mcp_call_failed", attempt=attempt, error=str(exc))
                if attempt <= self.config.max_retries:
                    await asyncio.sleep(self.config.retry_backoff_s * attempt)

        latency_ms = (time.monotonic() - start) * 1000
        log.error("mcp_dispatch_exhausted_retries", error=str(last_error))
        return ExecutionResult(ok=False, latency_ms=latency_ms, error=str(last_error), error_code="TRANSPORT_ERROR")
