"""
Gestión de riesgo con equity dinámico persistido en Firestore, en vez de un
capital hardcodeado en cada corrida. Calcula tamaño de posición por
conviction y expone si se superó el límite de pérdida diaria.

IMPORTANTE: esto lleva la cuenta de un equity simulado/de backtest, no debita
ninguna cuenta real de ningún exchange - este proyecto no ejecuta órdenes.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from config import constants as c

logger = logging.getLogger(__name__)

_EQUITY_COLLECTION = "account_state"
_EQUITY_DOC_ID = "latest"
_CACHE_TTL_SEG = 5.0


@dataclass(frozen=True, slots=True)
class EquityState:
    equity: float
    equity_inicio_dia: float
    perdida_diaria_pct: float
    actualizado: datetime

    @property
    def limite_diario_superado(self) -> bool:
        return self.perdida_diaria_pct >= c.DAILY_LOSS_LIMIT


class RiskManager:
    """Recibe el cliente de Firestore ya inicializado (el mismo que usa
    save_signal_to_firestore en order_flow_signal.py) en vez de crear uno
    propio - evita duplicar la lógica de autenticación ADC que ya está
    resuelta ahí. Si no hay cliente (Firestore no configurado), sigue
    funcionando con un equity local en memoria, no bloquea nada."""

    def __init__(self, firestore_client, capital_inicial: float = 50_000.0):
        self._db = firestore_client
        self._capital_inicial = capital_inicial
        self._lock = asyncio.Lock()
        self._cache: Optional[EquityState] = None
        self._cache_ts = 0.0

    async def get_equity_state(self) -> EquityState:
        ahora = asyncio.get_event_loop().time()
        if self._cache and (ahora - self._cache_ts) < _CACHE_TTL_SEG:
            return self._cache

        if self._db is None:
            return self._estado_local_default()

        loop = asyncio.get_event_loop()
        doc_ref = self._db.collection(_EQUITY_COLLECTION).document(_EQUITY_DOC_ID)
        try:
            snap = await loop.run_in_executor(None, doc_ref.get)
        except Exception as e:
            logger.error(f"❌ Error leyendo equity de Firestore (uso default en memoria): {e}")
            return self._estado_local_default()

        if snap.exists:
            data = snap.to_dict()
            equity = float(data.get("equity", self._capital_inicial))
            inicio_dia = float(data.get("equity_inicio_dia", equity))

            if self._es_equity_absurdo(equity) or self._es_equity_absurdo(inicio_dia):
                logger.warning(
                    f"⚠️ Equity absurdo en Firestore (equity={equity}, equity_inicio_dia={inicio_dia}, "
                    f"capital_base={self._capital_inicial}) - reseteando al capital base. Esto no debería "
                    f"pasar en operación normal, es un resto de alguna prueba con un PnL mal calculado."
                )
                equity = inicio_dia = self._capital_inicial
                try:
                    await loop.run_in_executor(None, lambda: doc_ref.set({
                        "equity": equity, "equity_inicio_dia": inicio_dia,
                        "trade_count": 0, "actualizado": datetime.now(timezone.utc).isoformat(),
                    }))
                except Exception as e:
                    logger.error(f"❌ Error reseteando equity corrupto en Firestore: {e}")
        else:
            equity = inicio_dia = self._capital_inicial
            try:
                await loop.run_in_executor(None, lambda: doc_ref.set({
                    "equity": equity, "equity_inicio_dia": inicio_dia,
                    "trade_count": 0, "actualizado": datetime.now(timezone.utc).isoformat(),
                }))
            except Exception as e:
                logger.error(f"❌ Error inicializando equity en Firestore: {e}")

        estado = self._construir_estado(equity, inicio_dia)
        self._cache = estado
        self._cache_ts = ahora
        return estado

    def _estado_local_default(self) -> EquityState:
        return self._construir_estado(self._capital_inicial, self._capital_inicial)

    def _es_equity_absurdo(self, equity: float) -> bool:
        """Detecta un equity fuera de cualquier rango plausible (negativo, o
        por fuera de [0.1x, 10x] el capital base) - típicamente un resto de
        una prueba vieja, no una pérdida/ganancia real de paper trading."""
        if equity <= 0:
            return True
        return not (self._capital_inicial * c.EQUITY_SANITY_MIN_MULT <= equity <= self._capital_inicial * c.EQUITY_SANITY_MAX_MULT)

    @staticmethod
    def _construir_estado(equity: float, inicio_dia: float) -> EquityState:
        perdida_pct = max(0.0, (inicio_dia - equity) / inicio_dia) if inicio_dia > 0 else 0.0
        return EquityState(equity, inicio_dia, perdida_pct, datetime.now(timezone.utc))

    def calcular_tamano_posicion(self, conviction: int, entry_price: float, sl_price: float, equity: float) -> float:
        """Tamaño en USDT (no en BTC - alimenta position_size_usdt del signal
        dict existente, no una cantidad para mandarle a ningún exchange)."""
        if entry_price == sl_price or equity <= 0 or entry_price <= 0:
            return 0.0
        risk_pct = c.RISK_POR_CONVICTION.get(conviction, c.RISK_PER_TRADE_BASE)
        riesgo_usdt = equity * risk_pct
        dist_pct = abs(entry_price - sl_price) / entry_price
        if dist_pct <= 0:
            return 0.0

        tamano = riesgo_usdt / dist_pct
        # Con un SL muy ajustado, "riesgo fijo en USD" pide un notional enorme
        # (matemáticamente correcto, pero fuera de lo operable). Cap duro:
        # nunca más del MAX_POSITION_EQUITY_PCT del equity en una sola
        # posición - el riesgo real queda por debajo del risk_pct nominal
        # en ese caso, lo cual está bien (nunca al revés).
        max_notional = equity * c.MAX_POSITION_EQUITY_PCT
        if tamano > max_notional:
            logger.warning(
                f"⚠️ Position size capped to equity limit: {max_notional:.2f} USDT "
                f"(risk hubiera sido >{risk_pct:.2%})"
            )
            tamano = max_notional
        return round(tamano, 2)

    async def registrar_resultado(self, pnl: float) -> EquityState:
        """Actualiza el equity tras cerrar un trade simulado/manual."""
        async with self._lock:
            estado_actual = await self.get_equity_state()
            nuevo_equity = estado_actual.equity + pnl

            if self._es_equity_absurdo(nuevo_equity):
                logger.error(
                    f"❌ registrar_resultado(pnl={pnl}) llevaría el equity a {nuevo_equity} - "
                    f"fuera de rango plausible, probablemente un PnL mal calculado (bug de PaperEngine, "
                    f"no una pérdida real). Se ignora el pnl y el equity queda sin cambios."
                )
                return estado_actual

            if self._db is not None:
                loop = asyncio.get_event_loop()
                doc_ref = self._db.collection(_EQUITY_COLLECTION).document(_EQUITY_DOC_ID)
                try:
                    await loop.run_in_executor(None, lambda: doc_ref.update({
                        "equity": nuevo_equity,
                        "actualizado": datetime.now(timezone.utc).isoformat(),
                    }))
                except Exception as e:
                    logger.error(f"❌ Error actualizando equity en Firestore: {e}")

            estado = self._construir_estado(nuevo_equity, estado_actual.equity_inicio_dia)
            self._cache = estado
            self._cache_ts = asyncio.get_event_loop().time()
            return estado

    async def resetear_dia(self, nuevo_equity: Optional[float] = None) -> None:
        async with self._lock:
            equity = nuevo_equity if nuevo_equity is not None else (self._cache.equity if self._cache else self._capital_inicial)
            if self._db is not None:
                loop = asyncio.get_event_loop()
                doc_ref = self._db.collection(_EQUITY_COLLECTION).document(_EQUITY_DOC_ID)
                try:
                    await loop.run_in_executor(None, lambda: doc_ref.set({
                        "equity": equity, "equity_inicio_dia": equity,
                        "trade_count": 0, "actualizado": datetime.now(timezone.utc).isoformat(),
                    }))
                except Exception as e:
                    logger.error(f"❌ Error reseteando equity en Firestore: {e}")
            self._cache = None
