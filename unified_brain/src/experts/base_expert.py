from __future__ import annotations

from abc import ABC, abstractmethod

from src.schemas.market import MarketSnapshot
from src.schemas.signals import UnifiedSignal


class BaseExpert(ABC):
    """Ambos expertos son deterministas y sincronos (asi son sus decide()
    reales) -- el orchestrator los corre en asyncio.to_thread para no
    bloquear el loop, no porque hagan I/O aca."""

    @abstractmethod
    def analyze(self, snapshot: MarketSnapshot) -> UnifiedSignal | None:
        """None significa 'no hay setup para mi estilo', no 'no operar' -- esa
        decision es del router (deterministic_router.py), no del experto."""
        raise NotImplementedError


class NullExpert(BaseExpert):
    """Siempre devuelve None -- usado para desactivar uno de los dos
    expertos (ej: correr solo scalping en una cuenta chica donde swing
    todavia no esta validado) sin tocar SignalOrchestrator, que requiere
    ambos expertos no-None en su constructor."""

    def analyze(self, snapshot: MarketSnapshot) -> UnifiedSignal | None:
        return None
