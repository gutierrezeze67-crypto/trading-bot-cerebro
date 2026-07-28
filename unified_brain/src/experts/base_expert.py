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
