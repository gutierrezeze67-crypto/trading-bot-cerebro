"""Contrato unico de salida para ambos expertos (scalping y swing).

Los motores reales (src/order_flow_signal.decide y
strategies/htf_funding_btc/src/brain_htf_funding.HTFFundingBrain.decide)
devuelven dicts con formas distintas entre si (ver adaptadores en
unified_brain/experts/). UnifiedSignal es la forma comun que ve todo lo
que esta rio abajo (router, risk engine, dispatcher).
"""
from __future__ import annotations

import uuid
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator

StrategyType = Literal["swing", "scalping"]
Direction = Literal["LONG", "SHORT"]


class PatternName(str, Enum):
    """Union de los patrones que detectan los dos motores reales.

    scalping (src/order_flow_signal.py, funcion _prioridad_setup): STOP_RUN,
    ABSORPTION, DELTA_DIV, INITIATIVE_PULLBACK, BREAKOUT_VOL, IMBALANCE_STACK,
    ICEBERG, LIQUIDITY_POOL.
    swing (brain_htf_funding.py, funcion prioridad_setup): STOP_RUN,
    ABSORPTION, DELTA_DIV, INITIATIVE_PULLBACK, BREAKOUT_VOL (subconjunto,
    sin ICEBERG/IMBALANCE_STACK: requieren order book L2 que el swing no usa).
    """

    STOP_RUN = "STOP_RUN"
    ABSORPTION = "ABSORPTION"
    DELTA_DIV = "DELTA_DIV"
    INITIATIVE_PULLBACK = "INITIATIVE_PULLBACK"
    BREAKOUT_VOL = "BREAKOUT_VOL"
    IMBALANCE_STACK = "IMBALANCE_STACK"
    ICEBERG = "ICEBERG"
    LIQUIDITY_POOL = "LIQUIDITY_POOL"


class UnifiedSignal(BaseModel):
    signal_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    strategy_type: StrategyType
    pattern_name: PatternName
    direction: Direction
    entry_price: float = Field(gt=0)
    sl_price: float = Field(gt=0)
    tp_price: float = Field(gt=0, description="Primer target (TP1). El resto del schedule vive en metadata['tp_schedule'].")
    confidence: float = Field(ge=0.0, le=1.0, description="conviction/10 tal cual la devuelve el motor real (escala 0-10)")
    winrate_historical: float | None = Field(default=None, ge=0.0, le=1.0)
    reasoning: str = ""
    target_rr: float = Field(gt=0)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("tp_price")
    @classmethod
    def _tp_distinto_de_entry(cls, v: float, info) -> float:
        entry = info.data.get("entry_price")
        if entry is not None and v == entry:
            raise ValueError("tp_price no puede ser igual a entry_price")
        return v

    @property
    def sl_distance(self) -> float:
        return abs(self.entry_price - self.sl_price)

    @property
    def tp_distance(self) -> float:
        return abs(self.tp_price - self.entry_price)
