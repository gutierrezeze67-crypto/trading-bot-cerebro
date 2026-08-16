"""API de trading nueva y separada de orderflow_backend/ a proposito.

orderflow_backend/src/main.py es un motor de DATOS de mercado puro (candle_update,
candle_closed, big_trade, tape_speed) en el puerto 8000 (ver su README) -- no
tiene ni un solo evento de riesgo u ordenes, ni conoce cuentas ni posiciones.
Esta API es la pieza de trading/ejecucion, corre en OTRO puerto (8001) para
no chocar.

Rutea por user_id (/ws/{user_id} y /api/*/{user_id}) para que trading_frontend/,
Google AI Studio, o cualquier cliente pueda consumir el estado real de un
usuario -- hoy solo hay un "user_id" real corriendo (el que arranca
main_orchestrator.py), pero la routing ya queda lista para mas de uno.

DOS FORMAS DE CONSUMIR LO MISMO:
  - WS /ws/{user_id}: push en tiempo real, formato {"event": <str>, "data": {...}}.
  - REST /api/*/{user_id}: polling, para clientes que no hablan WebSocket
    (ej. prototipos de Google AI Studio). NO son datos separados/mockeados --
    ConnectionManager cachea el ULTIMO evento real de cada tipo que ya paso
    por broadcast() (el mismo dato que reciben los clientes WS), y los
    endpoints REST simplemente lo devuelven. Si no llego ningun evento
    todavia, se devuelve un default honesto (equity=None, trades=[], etc),
    nunca un numero inventado.

Eventos que emite signal_orchestrator.py: trade_executed, risk_update,
advisor_update, signal_blocked, error.

COMANDOS reales (via WS {"cmd": ...} o POST /api/command/{user_id}):
  - pause / resume: pega directo a ConnectionManager.set_paused(), que
    main_orchestrator.run_market_loop() consulta antes de cada tick. Pausar
    por REST pausa el mismo loop que pausar por WS -- un solo estado.
  - config: overrides de risk_pct/max_trades/max_dd. Se guardan en
    ConnectionManager y SignalOrchestrator los aplica en cada
    pre_flight() via risk_overrides_provider (ver signal_orchestrator.py) --
    tambien real, no un dict que nadie lee.

Uso: python main_orchestrator.py (corrido desde dentro de unified_brain/)
"""
from __future__ import annotations

import asyncio
import json
import os
from collections import defaultdict, deque
from typing import Any

import structlog
from fastapi import Depends, FastAPI, Header, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

logger = structlog.get_logger(__name__)

# Herramienta de uso personal -- ningun endpoint de datos/comandos debe ser
# alcanzable sin esta clave (BACKEND_API_KEY en unified_brain/.env). Falla al
# arrancar si no esta seteada: mejor un crash explicito en el arranque que un
# servidor sirviendo balance/comandos reales sin ninguna proteccion.
BACKEND_API_KEY = os.environ.get("BACKEND_API_KEY")
if not BACKEND_API_KEY:
    raise RuntimeError("Falta BACKEND_API_KEY en el entorno -- ver unified_brain/.env")


def require_api_key(x_api_key: str | None = Header(default=None, alias="X-API-Key")) -> None:
    if x_api_key != BACKEND_API_KEY:
        raise HTTPException(status_code=401, detail="API key invalida o faltante")

MAX_TRADE_HISTORY = 200
RISK_OVERRIDE_KEYS = {
    "riskPct": "risk_pct_per_trade",
    "maxTrades": "max_trades_per_day",
    "maxDD": "max_daily_loss_pct",
}


class ConnectionManager:
    def __init__(self) -> None:
        self._clients: dict[str, set[WebSocket]] = defaultdict(set)
        self._lock = asyncio.Lock()
        self._paused: dict[str, bool] = defaultdict(bool)

        # Cache del ultimo evento real de cada tipo, para servir /api/* sin
        # duplicar estado -- se llena como efecto secundario de broadcast().
        self._latest_risk: dict[str, dict[str, Any]] = {}
        self._latest_advisor: dict[str, dict[str, Any]] = {}
        self._trade_history: dict[str, deque[dict[str, Any]]] = defaultdict(lambda: deque(maxlen=MAX_TRADE_HISTORY))

        # Overrides de risk config por usuario, seteados via cmd=config.
        # SignalOrchestrator los lee en cada tick (risk_overrides_provider).
        self._risk_overrides: dict[str, dict[str, float]] = defaultdict(dict)

    async def connect(self, user_id: str, websocket: WebSocket) -> None:
        await websocket.accept()
        async with self._lock:
            self._clients[user_id].add(websocket)

    async def disconnect(self, user_id: str, websocket: WebSocket) -> None:
        async with self._lock:
            self._clients[user_id].discard(websocket)

    async def broadcast(self, user_id: str, event_type: str, data: dict[str, Any]) -> None:
        self._cache_event(user_id, event_type, data)
        message = {"event": event_type, "data": data}
        for websocket in list(self._clients.get(user_id, ())):
            try:
                await websocket.send_json(message)
            except Exception:  # noqa: BLE001 - la limpieza real ocurre al desconectar
                await self.disconnect(user_id, websocket)

    def _cache_event(self, user_id: str, event_type: str, data: dict[str, Any]) -> None:
        if event_type == "risk_update":
            self._latest_risk[user_id] = data
        elif event_type == "advisor_update":
            self._latest_advisor[user_id] = data
        elif event_type == "trade_executed":
            self._trade_history[user_id].appendleft(data)

    def get_latest_risk(self, user_id: str) -> dict[str, Any] | None:
        return self._latest_risk.get(user_id)

    def get_latest_advisor(self, user_id: str) -> dict[str, Any] | None:
        return self._latest_advisor.get(user_id)

    def get_trade_history(self, user_id: str, limit: int) -> list[dict[str, Any]]:
        return list(self._trade_history.get(user_id, ()))[:limit]

    def get_open_tickets(self, user_id: str) -> list[int]:
        return [t["ticket"] for t in self._trade_history.get(user_id, ()) if t.get("status") == "OPEN" and t.get("ticket")]

    def update_trade_status(self, user_id: str, ticket: int, status: str, pnl: float | None) -> bool:
        """Muta en el lugar la entrada del deque que matchea `ticket` -- ver
        reconcile_trade_status_loop() en main_orchestrator.py, que es quien
        llama esto tras confirmar contra MT5 real si el trade sigue abierto o
        ya cerro. signal_orchestrator.py nunca actualiza esto al emitir
        trade_executed (status queda 'OPEN' fijo a proposito, ver comentario
        ahi) -- este es el unico lugar que lo corrige despues, con datos
        reales del broker en vez de inventados."""
        for trade in self._trade_history.get(user_id, ()):
            if trade.get("ticket") == ticket:
                trade["status"] = status
                trade["pnl"] = pnl
                return True
        return False

    def is_paused(self, user_id: str) -> bool:
        return self._paused[user_id]

    def set_paused(self, user_id: str, paused: bool) -> None:
        self._paused[user_id] = paused

    def get_risk_overrides(self, user_id: str) -> dict[str, float]:
        return dict(self._risk_overrides.get(user_id, {}))

    def set_risk_override(self, user_id: str, key: str, value: float) -> None:
        self._risk_overrides[user_id][key] = value


manager = ConnectionManager()

app = FastAPI(title="Unified Brain Trading API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.get("/")
async def root() -> dict[str, str]:
    return {"status": "ok", "service": "unified_brain"}


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.websocket("/ws/{user_id}")
async def ws_trading(websocket: WebSocket, user_id: str) -> None:
    # WS del navegador no puede mandar headers custom -- la clave viaja como
    # query param (?api_key=...). Se rechaza ANTES de aceptar la conexion.
    if websocket.query_params.get("api_key") != BACKEND_API_KEY:
        await websocket.close(code=4401)
        return
    await manager.connect(user_id, websocket)
    try:
        while True:
            raw = await websocket.receive_text()
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue
            await _handle_command(user_id, msg)
    except WebSocketDisconnect:
        pass
    except Exception as exc:  # noqa: BLE001
        logger.warning("ws_trading_error", user_id=user_id, error=str(exc))
    finally:
        await manager.disconnect(user_id, websocket)


async def _handle_command(user_id: str, msg: dict[str, Any]) -> dict[str, Any]:
    """Compartido entre el WS ({"cmd": ...}) y POST /api/command/{user_id} --
    un solo lugar que efectivamente toca manager, para que pausar/configurar
    haga lo mismo sin importar por que canal llego el comando."""
    cmd = msg.get("cmd")

    if cmd == "pause":
        manager.set_paused(user_id, True)
        logger.info("trading_paused", user_id=user_id)
        await manager.broadcast(user_id, "risk_update", {**(manager.get_latest_risk(user_id) or {}), "status": "PAUSED"})
        return {"status": "paused"}

    if cmd == "resume":
        manager.set_paused(user_id, False)
        logger.info("trading_resumed", user_id=user_id)
        await manager.broadcast(user_id, "risk_update", {**(manager.get_latest_risk(user_id) or {}), "status": "OK"})
        return {"status": "resumed"}

    if cmd == "acknowledge_block":
        # No hay estado de "bloqueo reconocido" que trackear todavia
        # (RiskEngine bloquea de nuevo el proximo tick igual si las
        # condiciones siguen -- esto es solo para que el frontend pueda
        # descartar el toast). Placeholder intencional.
        logger.info("block_acknowledged", user_id=user_id, code=msg.get("code"))
        return {"status": "acknowledged"}

    if cmd == "config":
        applied: dict[str, float] = {}
        for payload_key, config_field in RISK_OVERRIDE_KEYS.items():
            if payload_key in msg:
                value = float(msg[payload_key])
                manager.set_risk_override(user_id, config_field, value)
                applied[config_field] = value
        logger.info("risk_override_applied", user_id=user_id, applied=applied)
        return {"status": "ok", "risk_overrides": manager.get_risk_overrides(user_id)}

    return {"status": "unknown_command", "cmd": cmd}


# ----------------------------------------------------------------------
# REST -- mismos datos que WS, para clientes que no hablan WebSocket
# (ej. prototipos de Google AI Studio). Ver docstring del modulo.
# ----------------------------------------------------------------------


@app.get("/api/state/{user_id}", dependencies=[Depends(require_api_key)])
async def get_state(user_id: str) -> dict[str, Any]:
    risk = manager.get_latest_risk(user_id)
    if risk is None:
        # Honesto: sin datos reales todavia (el orchestrator para ese
        # user_id no corrio ningun tick aun), no se inventa un balance.
        return {"status": "NO_DATA", "message": "sin datos todavia -- el orchestrator no emitio ningun risk_update para este user_id", "paused": manager.is_paused(user_id)}
    return {**risk, "paused": manager.is_paused(user_id)}


@app.get("/api/history/{user_id}", dependencies=[Depends(require_api_key)])
async def get_history(user_id: str, limit: int = 50) -> dict[str, Any]:
    trades = manager.get_trade_history(user_id, limit)
    return {"trades": trades, "limit": limit, "total": len(trades)}


@app.get("/api/advisor/{user_id}", dependencies=[Depends(require_api_key)])
async def get_advisor(user_id: str) -> dict[str, Any]:
    advisor = manager.get_latest_advisor(user_id)
    if advisor is None:
        return {"text": "Esperando señal...", "reasoning": "Esperando señal...", "type": "hold", "pattern": None, "hold_reason": None}
    return advisor


class CommandPayload(BaseModel):
    cmd: str
    code: str | None = None
    riskPct: float | None = Field(default=None, gt=0, le=1)
    maxTrades: int | None = Field(default=None, gt=0)
    maxDD: float | None = Field(default=None, gt=0, le=1)


@app.post("/api/command/{user_id}", dependencies=[Depends(require_api_key)])
async def send_command(user_id: str, payload: CommandPayload) -> dict[str, Any]:
    result = await _handle_command(user_id, payload.model_dump(exclude_none=True))
    return result


@app.get("/api/config/{user_id}", dependencies=[Depends(require_api_key)])
async def get_config(user_id: str) -> dict[str, Any]:
    risk = manager.get_latest_risk(user_id) or {}
    overrides = manager.get_risk_overrides(user_id)
    return {
        "paused": manager.is_paused(user_id),
        "max_trades_day": overrides.get("max_trades_per_day", risk.get("max_trades_day")),
        "max_daily_loss_pct": overrides.get("max_daily_loss_pct", risk.get("max_daily_loss_pct")),
        "risk_pct_per_trade": overrides.get("risk_pct_per_trade"),
        "overrides_active": overrides,
    }
