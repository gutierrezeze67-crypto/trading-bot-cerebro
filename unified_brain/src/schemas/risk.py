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
    max_position_equity_pct: float = Field(gt=0, le=1.0, default=0.95)
    contract_value_per_price_unit: float = Field(
        gt=0,
        default=1.0,
        description="USDT de PnL por 1 unidad de precio de movimiento y 1 unidad de qty. "
        "Para BTCUSDT (ASSET_CONFIG: contract_size=1, tick_size=tick_value=0.10) el ratio es 1.0 "
        "-- mismo valor que usa HTFRiskManager.calc_size(entry, sl) = risk_usdt/dist.",
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
