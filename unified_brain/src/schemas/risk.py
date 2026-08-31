"""Schemas del guardian de riesgo (unified_brain/risk/risk_engine.py).

AccountState es deliberadamente mas chico que EquityState (src/risk_manager.py)
o el dataclass HTFRiskManager (strategies/htf_funding_btc/src/risk_manager_htf.py):
es el minimo comun que el RiskEngine necesita para bloquear, sin importar de
cual de los dos motores (o de MT5 real via mcp_dispatcher) vino el numero.
"""
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

BlockCode = Literal[
    "OK",
    "MAX_DD",
    "MAX_TRADES",
    "MAX_POSITIONS",
    "SIZE_ANOMALY",
    "INVALID_SIGNAL",
]


class RiskConfig(BaseModel):
    max_daily_loss_pct: float = Field(gt=0, le=1.0, default=0.02)
    max_trades_per_day: int = Field(gt=0, default=5)
    max_concurrent_positions: int = Field(gt=0, default=1)
    risk_pct_per_trade: float = Field(gt=0, le=1.0, default=0.005)
    max_position_equity_pct: float = Field(
        gt=0,
        le=50.0,
        default=3.0,
        description="Tope de exposicion nocional como multiplo de equity. Antes 0.95 (95%),"
        " pensado para spot sin apalancamiento (Binance). En una cuenta CFD con margen"
        " (ej. Vantage, Exness) tener notional > equity es normal y seguro -- el limite real"
        " de riesgo lo pone risk_pct_per_trade o fixed_lot_override (perdida en SL), no el"
        " notional bruto. Tope subido de 10.0 a 50.0 (2026-08-31): con fixed_lot_override en"
        " una cuenta chica real (ej. 0.05-0.20 BTC en $200-$800 con apalancamiento ~1:400 de"
        " Exness BTCUSD) el ratio notional/equity real ronda 19-20x -- el limite viejo de 10x"
        " bloqueaba esto SIEMPRE, no como error real. 50.0 sigue actuando como backstop"
        " (cachea un bug tipo 'se mando 10x el lote querido'), simplemente calibrado al"
        " apalancamiento real del broker en vez de a un supuesto de cuenta spot.",
    )
    contract_value_per_price_unit: float = Field(
        gt=0,
        default=1.0,
        description="USDT de PnL por 1 unidad de precio de movimiento y 1 unidad de qty. "
        "Para BTCUSDT (ASSET_CONFIG: contract_size=1, tick_size=tick_value=0.10) el ratio es 1.0 "
        "-- mismo valor que usa HTFRiskManager.calc_size(entry, sl) = risk_usdt/dist.",
    )
    fixed_lot_override: float | None = Field(
        gt=0,
        default=None,
        description="Si esta seteado, pre_flight() usa este lote FIJO en vez de "
        "calcularlo desde risk_pct_per_trade*equity. Pensado para cuentas chicas "
        "donde el % de riesgo del modelo (0.5-0.75%) fuerza el lote minimo del "
        "broker de cualquier forma (ver sizing_capital_leverage.py) -- en vez de "
        "esa distorsion no controlada, se opera a un lote fijo elegido a mano y "
        "se sube manualmente mes a mes (0.05 -> 0.10 -> ...) a medida que la "
        "cuenta crece. SIZE_ANOMALY sigue aplicando como backstop de seguridad "
        "(notional vs max_position_equity_pct) aunque el lote sea fijo.",
    )


class AccountState(BaseModel):
    equity: float = Field(gt=0)
    equity_start_of_day: float = Field(gt=0)
    open_positions: int = Field(ge=0, default=0)
    trades_today: int = Field(ge=0, default=0)
    loss_streak: int = Field(ge=0, default=0, description="no viene de MT5 hoy -- mcp_dispatcher.get_account_state no lo deriva de history_deals_get todavia")

    # Campos que solo llena mcp_dispatcher.get_account_state() (cuenta MT5
    # real via MCP) -- quedan en None para AccountState construido desde
    # otras fuentes (tests).
    user_id: str | None = None
    balance: float | None = None
    free_margin: float | None = None
    margin_level: float | None = None
    realized_pnl_today: float | None = Field(default=None, description="suma de profit+swap+commission de history_deals_get de hoy -- informativo, RiskEngine sigue usando equity_start_of_day (incluye PnL flotante, mas conservador) para el bloqueo de MAX_DD")

    @property
    def daily_pnl_usdt(self) -> float:
        return self.equity - self.equity_start_of_day

    @property
    def daily_pnl_pct(self) -> float:
        return self.daily_pnl_usdt / self.equity_start_of_day


class RiskCheckResult(BaseModel):
    allowed: bool
    code: BlockCode
    msg: str
    adjusted_lot: float | None = None
    risk_cash: float | None = None
