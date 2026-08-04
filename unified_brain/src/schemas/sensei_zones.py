"""Estructura de datos pura para zonas Sensei (OB/FVG) -- SIN logica de
deteccion ni de trading aca. El generador que puebla estas zonas (porteado
del .pine real de "El Sensei v3") todavia no existe -- ver ZoneCache y
services/signal_orchestrator.py para el estado de la integracion.

Pydantic BaseModel (no @dataclass) para ser consistente con el resto de
unified_brain/src/schemas/ (UnifiedSignal, AccountState, RiskConfig son
todos BaseModel). Los campos runtime (touch_count, is_broken, mitigation)
son mutables -- Pydantic v2 permite reasignar atributos por default.

Timestamps en ts_ms (milisegundos), no ts_ns -- mismo criterio que
MarketSnapshot.ts_ms en market.py, para no introducir una segunda unidad de
tiempo en el mismo paquete.
"""
from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field


class SenseiZone(BaseModel):
    zone_id: str
    symbol: str
    side: Literal["BULLISH", "BEARISH"]
    price_high: float
    price_low: float
    mid_price: float
    probability: int = Field(ge=0, le=100, description="tasaCont final del generador -- 0 hasta que exista el generador real")
    quality_score: int = Field(ge=1, le=5)
    mitigation: Literal["UNTESTED", "PARTIAL", "FULL"] = "UNTESTED"
    timeframe_min: int
    trend: Literal["BULLISH", "BEARISH"]
    choch_active: bool = False
    choch_dir: Optional[Literal["UP", "DOWN"]] = None
    structure_aligned: bool = False
    range_pos_pct: float = Field(ge=0, le=1)
    legs: int = 0
    hold_rate_pct: float = Field(ge=0, le=100, default=50.0)
    learned_reach_atr: float = 2.0
    ts_ms: int
    metadata: dict = Field(default_factory=dict)

    # Runtime -- mutados por ZoneCache.mark_touch/mark_broken, no en la creacion
    touch_count: int = 0
    last_touch_ms: int = 0
    is_broken: bool = False
