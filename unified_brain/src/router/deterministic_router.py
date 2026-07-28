"""State machine determinista (DECISION EJECUTIVA del usuario: NO LLM aca).
El LLM/Gemini queda reservado para el panel Asesor / reportes, nunca para
esta decision de entrada en caliente.

Reglas (en orden de prioridad, la primera que aplica corta):
  1. Riesgo duro: DD >= limite o trades_today >= max -> hold. Este chequeo es
     redundante con RiskEngine.pre_flight (que corre DESPUES sobre la senal
     elegida) a proposito: doble seguro, el router no deberia ni intentar
     elegir un candidato si ya sabemos que risk_engine lo va a bloquear.
  2. Sin candidatos de ningun experto -> hold NO_PATTERN.
  3. Riesgo cercano al limite (>= risk_near_limit_ratio del limite de DD, o
     quedan <= trades_low_buffer trades del limite diario) -> descarta
     scalping (mas frecuencia = mas exposicion) salvo que sea el unico
     candidato; swing solo pasa si su confidence es alta.
  4. Si sobran 2 candidatos (scalping y swing, incluso en direcciones
     opuestas sobre el mismo simbolo -- nunca se envian los dos): si ambos
     tienen confidence > confidence_threshold_both, gana el de mayor
     Expected Value = winrate_historical_o_confidence * target_rr. Si no,
     desempata por el sesgo de sesion/volatilidad (overlap+ATR alto favorece
     scalping; fuera de overlap + precio cerca de nivel clave favorece swing),
     y si el sesgo no aplica a ninguno de los dos, tambien por EV.
"""
from __future__ import annotations

from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field

from src.schemas.risk import AccountState, RiskConfig
from src.schemas.signals import UnifiedSignal


class HoldReason(str, Enum):
    RISK_BLOCKED_DD = "RISK_BLOCKED_DD"
    RISK_BLOCKED_TRADES = "RISK_BLOCKED_TRADES"
    CHOPPY = "CHOPPY"
    NEWS = "NEWS"
    NO_PATTERN = "NO_PATTERN"
    SPREAD_WIDE = "SPREAD_WIDE"


class RouterContext(BaseModel):
    """Datos de sesion/mercado que el router usa solo como DESEMPATE, nunca
    como bloqueo duro. Los calcula quien arma el MarketSnapshot (orchestrator),
    el router no hace su propio computo de rolling stats."""

    hour_utc: int = Field(ge=0, le=23)
    overlap_session_start_utc: int = 13
    overlap_session_end_utc: int = 17
    atr_percentile: float | None = Field(default=None, ge=0, le=100, description="percentil del ATR actual vs su historia reciente")
    near_key_level: bool = Field(default=False, description="precio actual cerca de VAH/VAL/VWAP/POC (ver zone_proximity_pct de HTFParams)")
    spread_too_wide: bool = False
    news_blackout: bool = False

    @property
    def in_overlap_session(self) -> bool:
        return self.overlap_session_start_utc <= self.hour_utc <= self.overlap_session_end_utc


class RouterConfig(BaseModel):
    confidence_threshold_both: float = Field(ge=0, le=1, default=0.75)
    risk_near_limit_ratio: float = Field(ge=0, le=1, default=0.7, description="fraccion del max_daily_loss_pct desde la que se considera 'DD cercano'")
    trades_low_buffer: int = Field(ge=0, default=1, description="si quedan <= este numero de trades permitidos, solo se acepta el mejor setup")


class RouterDecision(BaseModel):
    action: Literal["scalping", "swing", "hold"]
    signal: UnifiedSignal | None = None
    hold_reason: HoldReason | None = None
    detail: str = ""


def _expected_value(signal: UnifiedSignal) -> float:
    wr = signal.winrate_historical if signal.winrate_historical is not None else signal.confidence
    return wr * signal.target_rr


class DeterministicRouter:
    def __init__(self, risk_config: RiskConfig | None = None, router_config: RouterConfig | None = None) -> None:
        self.risk_config = risk_config or RiskConfig()
        self.config = router_config or RouterConfig()

    def route(
        self,
        account: AccountState,
        scalp_signal: UnifiedSignal | None,
        swing_signal: UnifiedSignal | None,
        context: RouterContext,
    ) -> RouterDecision:
        # 1. Riesgo duro -- corta todo, ni mira los candidatos.
        if account.daily_pnl_pct <= -self.risk_config.max_daily_loss_pct:
            return RouterDecision(action="hold", hold_reason=HoldReason.RISK_BLOCKED_DD, detail=f"daily_pnl_pct={account.daily_pnl_pct:.2%}")
        if account.trades_today >= self.risk_config.max_trades_per_day:
            return RouterDecision(action="hold", hold_reason=HoldReason.RISK_BLOCKED_TRADES, detail=f"trades_today={account.trades_today}")
        if context.news_blackout:
            return RouterDecision(action="hold", hold_reason=HoldReason.NEWS, detail="news_blackout activo")
        if context.spread_too_wide:
            return RouterDecision(action="hold", hold_reason=HoldReason.SPREAD_WIDE, detail="spread por encima del maximo configurado")

        # 2. Sin candidatos.
        if scalp_signal is None and swing_signal is None:
            return RouterDecision(action="hold", hold_reason=HoldReason.NO_PATTERN, detail="ningun experto encontro setup")

        # 3. Riesgo cercano al limite -> restringe candidatos.
        dd_near_limit = account.daily_pnl_pct <= -(self.config.risk_near_limit_ratio * self.risk_config.max_daily_loss_pct)
        trades_low = (self.risk_config.max_trades_per_day - account.trades_today) <= self.config.trades_low_buffer

        candidates: list[tuple[Literal["scalping", "swing"], UnifiedSignal]] = []
        if scalp_signal is not None and not (dd_near_limit or trades_low):
            candidates.append(("scalping", scalp_signal))
        if swing_signal is not None:
            if not dd_near_limit or swing_signal.confidence >= self.config.confidence_threshold_both:
                candidates.append(("swing", swing_signal))

        if not candidates:
            reason_detail = "DD cercano al limite, descarta scalping y swing no llega a confidence minima" if dd_near_limit else "trades_today cerca del limite diario"
            return RouterDecision(action="hold", hold_reason=HoldReason.RISK_BLOCKED_TRADES if trades_low else HoldReason.RISK_BLOCKED_DD, detail=reason_detail)

        if len(candidates) == 1:
            name, signal = candidates[0]
            return RouterDecision(action=name, signal=signal, detail="unico candidato disponible tras filtros de riesgo")

        # 4. Dos candidatos (incluso en direcciones opuestas -- nunca se
        # envian ambos, siempre gana exactamente uno).
        (name_a, sig_a), (name_b, sig_b) = candidates
        both_confident = sig_a.confidence > self.config.confidence_threshold_both and sig_b.confidence > self.config.confidence_threshold_both

        if both_confident:
            winner = candidates[0] if _expected_value(sig_a) >= _expected_value(sig_b) else candidates[1]
            detail = f"ambos > {self.config.confidence_threshold_both} confidence, gana por EV (WR*RR)"
        else:
            session_favors_scalping = context.in_overlap_session and (context.atr_percentile or 0) > 50
            session_favors_swing = (not context.in_overlap_session) and context.near_key_level

            if session_favors_scalping and name_a == "scalping" or session_favors_scalping and name_b == "scalping":
                winner = candidates[0] if name_a == "scalping" else candidates[1]
                detail = "sesion overlap + ATR alto favorece scalping"
            elif session_favors_swing and (name_a == "swing" or name_b == "swing"):
                winner = candidates[0] if name_a == "swing" else candidates[1]
                detail = "fuera de overlap + precio cerca de nivel clave favorece swing"
            else:
                winner = candidates[0] if _expected_value(sig_a) >= _expected_value(sig_b) else candidates[1]
                detail = "sin sesgo de sesion aplicable, desempate por EV (WR*RR)"

        return RouterDecision(action=winner[0], signal=winner[1], detail=detail)
