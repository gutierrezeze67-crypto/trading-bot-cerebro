"""Bloqueador duro de riesgo. Corre SIEMPRE antes de mcp_dispatcher.execute(),
sin importar lo que haya decidido el router o el LLM asesor (si algun dia
se agrega uno) -- ver deterministic_router.py, que tambien aplica sus
propias reglas de riesgo en el routing, como segundo seguro redundante.

Formula de tamano de posicion identica a la que ya usa en produccion
strategies/htf_funding_btc/src/risk_manager_htf.HTFRiskManager.calc_size:
qty = risk_cash / sl_distance (para contract_value_per_price_unit=1.0,
que es el caso real de BTCUSDT hoy). Se generaliza el ratio para no
asumir contract_size=1 en otros simbolos.
"""
from __future__ import annotations

import structlog

from src.schemas.risk import AccountState, RiskCheckResult, RiskConfig
from src.schemas.signals import UnifiedSignal

logger = structlog.get_logger(__name__)


class RiskEngine:
    def __init__(self, config: RiskConfig | None = None) -> None:
        self.config = config or RiskConfig()

    def pre_flight(self, account: AccountState, signal: UnifiedSignal) -> RiskCheckResult:
        log = logger.bind(signal_id=signal.signal_id, strategy_type=signal.strategy_type)

        if account.daily_pnl_pct <= -self.config.max_daily_loss_pct:
            result = RiskCheckResult(
                allowed=False,
                code="MAX_DD",
                msg=f"Daily PnL {account.daily_pnl_pct:.2%} <= -{self.config.max_daily_loss_pct:.2%}",
            )
            log.warning("risk_blocked", code=result.code, msg=result.msg)
            return result

        if account.trades_today >= self.config.max_trades_per_day:
            result = RiskCheckResult(
                allowed=False,
                code="MAX_TRADES",
                msg=f"trades_today {account.trades_today} >= max_trades_per_day {self.config.max_trades_per_day}",
            )
            log.warning("risk_blocked", code=result.code, msg=result.msg)
            return result

        if account.open_positions >= self.config.max_concurrent_positions:
            result = RiskCheckResult(
                allowed=False,
                code="MAX_POSITIONS",
                msg=f"open_positions {account.open_positions} >= max_concurrent_positions {self.config.max_concurrent_positions}",
            )
            log.warning("risk_blocked", code=result.code, msg=result.msg)
            return result

        sl_distance = signal.sl_distance
        if sl_distance <= 0:
            result = RiskCheckResult(allowed=False, code="INVALID_SIGNAL", msg="sl_distance <= 0")
            log.warning("risk_blocked", code=result.code, msg=result.msg)
            return result

        risk_cash = account.equity * self.config.risk_pct_per_trade
        adjusted_lot = risk_cash / (sl_distance * self.config.contract_value_per_price_unit)

        notional = adjusted_lot * signal.entry_price * self.config.contract_value_per_price_unit
        if notional > account.equity * self.config.max_position_equity_pct:
            result = RiskCheckResult(
                allowed=False,
                code="SIZE_ANOMALY",
                msg=f"notional {notional:.2f} > {self.config.max_position_equity_pct:.0%} de equity ({account.equity:.2f})",
                risk_cash=risk_cash,
            )
            log.warning("risk_blocked", code=result.code, msg=result.msg)
            return result

        result = RiskCheckResult(allowed=True, code="OK", msg="pre_flight OK", adjusted_lot=adjusted_lot, risk_cash=risk_cash)
        log.info("risk_allowed", adjusted_lot=adjusted_lot, risk_cash=risk_cash)
        return result
