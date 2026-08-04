"""Ejecutor directo de MT5 (paquete `MetaTrader5`, sin pasar por MCP) --
camino de baja latencia para el envio de ordenes.

POR QUE EXISTE: mcp_dispatcher.py (via el subproceso mt5mcp) media ~433ms
por operacion. Diagnosticado en sesion: la causa real del pico de latencia
visto en pruebas (order_send tardando >10s en una direccion) fue una
condicion del lado del terminal MT5 (confirmacion de orden / configuracion),
no un parametro de la request -- con el terminal bien configurado
(One-Click Trading activo, "Confirm order requests" desactivado) la
latencia real medida baja a ~150-250ms. Ver conversacion para el
diagnostico completo.

TODAS las funciones de este modulo son SINCRONAS Y BLOQUEANTES -- el
paquete `MetaTrader5` no tiene variante async, `mt5.order_send()` bloquea
el hilo hasta que el broker responde. Quien llame a esto desde codigo
async (ver unified_brain/services/signal_orchestrator.py) DEBE envolver
cada llamada en `asyncio.to_thread(...)`. Llamarlo directo desde una
corutina congela el event loop entero -- API WS y el consumidor del
WebSocket de Binance en SnapshotEngine incluidos -- durante esos
~150-1500ms cada vez (segun broker, ver BROKER_CONFIG). NO sacar el
to_thread de signal_orchestrator.py pensando que ahorra latencia: el
overhead real de to_thread es de microsegundos, no de los ~5ms que a veces
se estima, y sacarlo cambia "bloquear un hilo" por "bloquear el proceso
entero" -- mala relacion costo/beneficio.

ARQUITECTURA HIBRIDA: mcp_dispatcher.py se sigue usando para LECTURAS
(get_account_state), este modulo se usa para EJECUCION
(execute_order/close_position). Ambos caminos abren su propia conexion IPC
al mismo terminal MT5 -- una desde el subproceso mt5mcp, otra desde este
proceso. MetaTrader5 soporta multiples clientes IPC simultaneos.

CONFIGURACION POR BROKER: la latencia y el comportamiento de order_send
varian por broker/tipo de ejecucion (ECN vs Market Maker interno). Vantage
(ECN) responde bien a IOC + deviation chico; Exness Standard (dealing desk
interno) necesita mas tolerancia (deviation mayor, FOK, price=0 para dejar
que el servidor complete al precio de mercado en vez de pedir un precio
exacto que el dealing desk puede rechazar). BROKER_CONFIG mapea esto por
nombre de servidor; config/broker_config.yaml (opcional) permite
sobreescribir/agregar brokers sin tocar este archivo.
"""
from __future__ import annotations

import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import MetaTrader5 as mt5
import structlog

logger = structlog.get_logger(__name__)

DEFAULT_BROKER_KEY = "default"
DEFAULT_SYMBOL = "BTCUSD"  # nunca "BTCUSDm" -- ver docstring del modulo

# Config hardcodeada de fallback -- config/broker_config.yaml (si existe) se
# mezcla encima de esto en tiempo de import, ver _load_broker_config_overrides.
# Todos los brokers de esta tabla operan BTCUSD (no varia por broker); el
# simbolo por-instancia vive en MT5DirectExecutor.symbol, no aca.
BROKER_CONFIG: dict[str, dict[str, Any]] = {
    DEFAULT_BROKER_KEY: {"deviation": 50, "filling": mt5.ORDER_FILLING_IOC, "price_style": "zero"},
    "vantage": {"deviation": 50, "filling": mt5.ORDER_FILLING_IOC, "price_style": "tick"},
    "exness": {"deviation": 100, "filling": mt5.ORDER_FILLING_FOK, "price_style": "zero"},
    "icmarkets": {"deviation": 30, "filling": mt5.ORDER_FILLING_IOC, "price_style": "zero"},
    "pepperstone": {"deviation": 30, "filling": mt5.ORDER_FILLING_IOC, "price_style": "zero"},
}

_FILLING_NAME_TO_CONST = {
    "IOC": mt5.ORDER_FILLING_IOC,
    "FOK": mt5.ORDER_FILLING_FOK,
    "RETURN": mt5.ORDER_FILLING_RETURN,
}


def _load_broker_config_overrides() -> dict[str, dict[str, Any]]:
    """Lee config/broker_config.yaml si existe. Opcional a proposito: si no
    esta, no parsea, o le falta algun campo, se usa BROKER_CONFIG tal cual
    esta arriba -- nunca rompe el arranque por un YAML mal escrito."""
    path = Path(__file__).resolve().parents[3] / "config" / "broker_config.yaml"
    if not path.exists():
        return {}
    try:
        import yaml

        with path.open(encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
    except Exception as exc:  # noqa: BLE001
        logger.warning("broker_config_yaml_load_failed", error=str(exc))
        return {}

    overrides: dict[str, dict[str, Any]] = {}
    for broker, params in (raw.get("brokers") or {}).items():
        if not isinstance(params, dict):
            continue
        default = BROKER_CONFIG[DEFAULT_BROKER_KEY]
        overrides[str(broker).lower()] = {
            "deviation": int(params.get("deviation", default["deviation"])),
            "filling": _FILLING_NAME_TO_CONST.get(str(params.get("filling", "IOC")).upper(), default["filling"]),
            "price_style": params.get("price_style", default["price_style"]),
        }
    return overrides


BROKER_CONFIG.update(_load_broker_config_overrides())


def _detect_broker(server: str) -> str:
    server_lower = (server or "").lower()
    for name in BROKER_CONFIG:
        if name != DEFAULT_BROKER_KEY and name in server_lower:
            return name
    return DEFAULT_BROKER_KEY


@dataclass
class OrderResult:
    success: bool
    ticket: int = 0
    price: float = 0.0
    volume: float = 0.0
    latency_ms: float = 0.0
    error: str = ""
    retcode: int = 0
    comment: str = ""


class MT5DirectExecutor:
    """Conexion directa, sin reconexion automatica agresiva -- reconectar
    en cada llamada fue justamente parte de lo que agregaba latencia. Se
    conecta una vez y se mantiene viva durante toda la sesion; is_connected()
    solo verifica el estado, no reconecta por si sola (ver connect())."""

    def __init__(self, login: int, password: str, server: str, path: str | None = None, symbol: str | None = None) -> None:
        self.login = login
        self.password = password
        self.server = server
        self.path = path or r"C:\Program Files\MetaTrader 5\terminal64.exe"
        self._connected = False

        self.broker = _detect_broker(server)
        self.broker_config = BROKER_CONFIG.get(self.broker, BROKER_CONFIG[DEFAULT_BROKER_KEY])

        # Simbolo default para llamadas que no especifiquen uno -- MT5_SYMBOL
        # gana si esta seteado, si no el que se pase al constructor, si no
        # DEFAULT_SYMBOL. NUNCA "BTCUSDm".
        self.symbol = symbol or os.environ.get("MT5_SYMBOL", DEFAULT_SYMBOL)

        # Constantes que no varian por broker.
        self.config = {"type_time": mt5.ORDER_TIME_GTC, "magic": 999999}

        logger.info(
            "mt5_direct_executor_init", broker=self.broker, symbol=self.symbol,
            deviation=self.broker_config["deviation"], price_style=self.broker_config["price_style"],
        )

    def connect(self, symbol: str | None = None) -> bool:
        """symbol: usado solo para el warmup post-conexion (ver _warmup),
        no limita que simbolos se puedan operar despues. Default self.symbol
        (MT5_SYMBOL / DEFAULT_SYMBOL, ver __init__) si no se pasa uno."""
        symbol = symbol or self.symbol
        try:
            mt5.shutdown()
            time.sleep(0.5)

            if not mt5.initialize(path=self.path):
                logger.error("mt5_direct_init_failed", error=str(mt5.last_error()))
                return False

            if not mt5.login(self.login, self.password, self.server):
                logger.error("mt5_direct_login_failed", error=str(mt5.last_error()))
                mt5.shutdown()
                return False

            info = mt5.account_info()
            if not info or info.balance <= 0:
                logger.error("mt5_direct_no_account_info")
                mt5.shutdown()
                return False

            self._connected = True
            logger.info(
                "mt5_direct_connected", server=self.server, balance=info.balance,
                broker_detected=self.broker, broker_config=self.broker_config,
            )
            self._warmup(symbol)
            return True
        except Exception as exc:  # noqa: BLE001
            logger.error("mt5_direct_connect_error", error=str(exc))
            return False

    def _warmup(self, symbol: str) -> None:
        """Pre-calienta la conexion con 3 ticks + 2 order_check (dry-run,
        NO coloca ninguna orden real) para que la primera orden de verdad
        no pague el costo de la primera resolucion de simbolo/cache interno
        de MT5. Silencioso a proposito: solo logea si algo falla, nunca
        lanza -- un warmup fallido no debe frenar connect()."""
        try:
            for _ in range(3):
                mt5.symbol_info_tick(symbol)
                time.sleep(0.02)

            tick = mt5.symbol_info_tick(symbol)
            if not tick:
                return

            check_request = {
                "action": mt5.TRADE_ACTION_DEAL,
                "symbol": symbol,
                "volume": 0.01,
                "type": mt5.ORDER_TYPE_BUY,
                "price": self._resolve_price(tick, side_is_buy=True),
                "deviation": self.broker_config["deviation"],
                "magic": self.config["magic"],
                "type_time": self.config["type_time"],
                "type_filling": self.broker_config["filling"],
            }
            for _ in range(2):
                mt5.order_check(check_request)

            logger.debug("mt5_direct_warmup_done", symbol=symbol, broker=self.broker)
        except Exception as exc:  # noqa: BLE001
            logger.warning("mt5_direct_warmup_failed", symbol=symbol, error=str(exc))

    def _resolve_price(self, tick: Any, side_is_buy: bool) -> float:
        if self.broker_config.get("price_style") == "zero":
            return 0.0
        return tick.ask if side_is_buy else tick.bid

    def is_connected(self) -> bool:
        if not self._connected:
            return False
        try:
            info = mt5.account_info()
            self._connected = bool(info and info.balance > 0)
            return self._connected
        except Exception:  # noqa: BLE001
            self._connected = False
            return False

    def execute_order(
        self, symbol: str | None = None, side: str = "BUY", volume: float = 0.01,
        sl: float = 0, tp: float = 0, comment: str = "", deviation: int | None = None,
    ) -> OrderResult:
        symbol = symbol or self.symbol
        if not self.is_connected():
            return OrderResult(success=False, error="No conectado a MT5")

        try:
            tick = mt5.symbol_info_tick(symbol)
            if not tick:
                return OrderResult(success=False, error=f"No se obtuvo tick para {symbol}")

            side_upper = side.upper()
            if side_upper == "BUY":
                order_type, side_is_buy = mt5.ORDER_TYPE_BUY, True
            elif side_upper == "SELL":
                order_type, side_is_buy = mt5.ORDER_TYPE_SELL, False
            else:
                return OrderResult(success=False, error=f"side invalido: {side!r} (debe ser BUY o SELL)")

            request: dict[str, Any] = {
                "action": mt5.TRADE_ACTION_DEAL,
                "symbol": symbol,
                "volume": float(volume),
                "type": order_type,
                "price": self._resolve_price(tick, side_is_buy),
                "deviation": deviation if deviation is not None else self.broker_config["deviation"],
                "magic": self.config["magic"],
                "comment": comment or f"UOB_{int(time.time())}",
                "type_time": self.config["type_time"],
                "type_filling": self.broker_config["filling"],
            }
            if sl > 0:
                request["sl"] = float(sl)
            if tp > 0:
                request["tp"] = float(tp)

            start = time.perf_counter()
            result = mt5.order_send(request)
            latency_ms = (time.perf_counter() - start) * 1000

            if result is None:
                return OrderResult(success=False, error=f"order_send None: {mt5.last_error()}", latency_ms=latency_ms)

            if result.retcode == mt5.TRADE_RETCODE_DONE:
                logger.info(
                    "mt5_direct_order_executed", symbol=symbol, side=side, broker=self.broker,
                    ticket=result.order, latency_ms=latency_ms,
                )
                return OrderResult(
                    success=True, ticket=result.order, price=result.price, volume=result.volume,
                    latency_ms=latency_ms, retcode=result.retcode, comment=result.comment,
                )
            logger.warning("mt5_direct_order_rejected", retcode=result.retcode, comment=result.comment)
            return OrderResult(
                success=False, error=f"retcode {result.retcode}: {result.comment}",
                latency_ms=latency_ms, retcode=result.retcode, comment=result.comment,
            )
        except Exception as exc:  # noqa: BLE001
            logger.error("mt5_direct_execute_error", error=str(exc))
            return OrderResult(success=False, error=str(exc))

    def close_position(self, ticket: int, deviation: int | None = None) -> OrderResult:
        if not self.is_connected():
            return OrderResult(success=False, error="No conectado a MT5")

        try:
            positions = mt5.positions_get(ticket=ticket)
            if not positions:
                return OrderResult(success=False, error=f"Posicion {ticket} no encontrada")
            pos = positions[0]

            tick = mt5.symbol_info_tick(pos.symbol)
            if not tick:
                return OrderResult(success=False, error=f"No se obtuvo tick para {pos.symbol}")

            if pos.type == mt5.POSITION_TYPE_BUY:
                close_type, side_is_buy = mt5.ORDER_TYPE_SELL, False
            else:
                close_type, side_is_buy = mt5.ORDER_TYPE_BUY, True

            request: dict[str, Any] = {
                "action": mt5.TRADE_ACTION_DEAL,
                "symbol": pos.symbol,
                "volume": pos.volume,
                "type": close_type,
                "position": ticket,
                "price": self._resolve_price(tick, side_is_buy),
                "deviation": deviation if deviation is not None else self.broker_config["deviation"],
                "magic": pos.magic,
                "comment": "CLOSE",
                "type_time": self.config["type_time"],
                "type_filling": self.broker_config["filling"],
            }

            start = time.perf_counter()
            result = mt5.order_send(request)
            latency_ms = (time.perf_counter() - start) * 1000

            if result is None:
                return OrderResult(success=False, error=f"close None: {mt5.last_error()}", latency_ms=latency_ms)

            if result.retcode == mt5.TRADE_RETCODE_DONE:
                logger.info("mt5_direct_position_closed", ticket=ticket, latency_ms=latency_ms)
                return OrderResult(
                    success=True, ticket=result.order, price=result.price, volume=result.volume,
                    latency_ms=latency_ms, retcode=result.retcode, comment=result.comment,
                )
            return OrderResult(
                success=False, error=f"retcode {result.retcode}: {result.comment}",
                latency_ms=latency_ms, retcode=result.retcode, comment=result.comment,
            )
        except Exception as exc:  # noqa: BLE001
            logger.error("mt5_direct_close_error", error=str(exc))
            return OrderResult(success=False, error=str(exc))

    def get_account_info(self) -> dict[str, Any] | None:
        if not self.is_connected():
            return None
        info = mt5.account_info()
        if not info:
            return None
        return {
            "balance": info.balance, "equity": info.equity, "margin": info.margin,
            "free_margin": info.margin_free, "profit": info.profit,
            "currency": info.currency, "server": info.server, "login": info.login,
        }

    def get_positions(self) -> list[Any]:
        if not self.is_connected():
            return []
        positions = mt5.positions_get()
        return list(positions) if positions else []

    def disconnect(self) -> None:
        try:
            mt5.shutdown()
        except Exception:  # noqa: BLE001
            pass
        self._connected = False
        logger.info("mt5_direct_disconnected")
