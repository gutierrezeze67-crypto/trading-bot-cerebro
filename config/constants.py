"""
Umbrales y parámetros ajustables del motor determinista. A diferencia de
settings.py (credenciales, gitignored), este archivo no tiene secretos y sí
se versiona - es el lugar para tocar números sin tener que buscarlos
desperdigados por el código.
"""

# Mercado
SYMBOL = "BTCUSDT"
TICK_SIZE = 0.1
STEP_SIZE = 0.001

# HTF Gate: minutos de datos EN VIVO (no históricos) necesarios antes de
# operar. Antes de esto, run_ciclo_live usaba el volume profile histórico
# como puente - ahora, para el motor determinista, no hay puente: sin esto
# no se opera, punto.
HTF_READY_MINUTES = 30

# Riesgo
SL_OFFSET_ATR_MULT = 0.5
DAILY_LOSS_LIMIT = 0.025
RISK_PER_TRADE_BASE = 0.005
MAX_POSITION_EQUITY_PCT = 0.95  # techo duro: nunca más del 95% del equity en una sola posición, aunque el SL sea muy ajustado y el risk_pct nominal pida más notional
RISK_POR_CONVICTION = {6: 0.005, 7: 0.005, 8: 0.00625, 9: 0.00625, 10: 0.0075}
TIME_STOP_TP1_MINUTES = 15
TIME_STOP_CLOSE_MINUTES = 30

# Validador Gemini (ya no decide, solo valida/narra)
GEMINI_VALIDATOR_TIMEOUT_MS = 800

# Detectores v2
IMBALANCE_RATIO = 3.0
ICEBERG_MIN_UPDATES = 2
ICEBERG_WINDOW_SECONDS = 2
ABSORPTION_PERCENTILE = 95
ABSORPTION_FLIP_RATIO = 0.35
ABSORPTION_MIN_STALL_TICKS = 3

# Confluencia HTF: distancia máxima al POC/VAL/VAH para considerar que el
# precio está "en la zona" (en vez de en No Man's Land)
HTF_ZONE_DISTANCE_PCT = 0.003

# Filtros institucionales: reducen ruido operando solo en zonas donde el
# dinero grande decide (POC/VAH/VAL/Swing H4).
INSTITUTIONAL_ZONE_TOLERANCE_PCT = 0.0015  # 0.15% de distancia a zona (~90 ticks BTC)
ZONE_COOLDOWN_HOURS = 0.5                  # horas de bloqueo tras operar una zona (30 min - antes 4h bloqueaba casi todo el flujo, ver nota abajo)
ATR_30M_PERIODS = 14                       # períodos de ATR en velas M30 (usado por snapshot_engine para el HTF, no por decide())
SWING_H4_LOOKBACK = 96                     # velas H4 para swing high/low (4 días)

# TP1/TP2/SL FIJOS en distancia de precio desde el entry (no desde la zona
# HTF ni escalados por ATR) - gestión híbrida: TP1 cierra 50% de la
# posición, TP2 es el corredor con el 50% restante y SL movido a breakeven.
# Reemplaza a los filtros CORE_MIN_RR/CORE_TP_USDT_MIN-MAX/MIN_RR_ATR_MULTIPLIER
# (eliminados): con un run real de 100+ min se confirmó que el cooldown de
# 4h + el TP atado a la zona HTF (raro que caiga en 300-600 USDT Y esté
# alineado con un nivel real) dejaban el sistema en 0 trades casi siempre -
# ver decide()/_calcular_niveles().
TP1_PIPS = 500.0             # $500 de distancia de precio ≈ 0.77% @ 65k (RR 2:1, validado en backtest_standard_account.py: WR 57.7%/PF 1.43 Exness, PF 1.59 Vantage - ver results/btc_standard_account_wf_*.json)
TP2_PIPS = 750.0             # $750 (corredor) ≈ 1.15% @ 65k (RR 3:1)
SL_PIPS = 250.0              # SL inicial: 1:2 con TP1 (antes 1:1 con TP1=250 - ese config daba PF 1.23/expectancy $63.49 vs $121.93 de este, ver conversacion)
BREAKEVEN_BUFFER_PIPS = 50.0  # tras TP1, el SL del resto se mueve a entry +/- esto

# Guard-rail de equity: un equity absurdo en Firestore (típicamente restos de
# una prueba vieja con un PnL mal calculado) no debe poder bloquear el
# sistema real vía el daily loss limit - ver risk_manager.get_equity_state().
EQUITY_SANITY_MIN_MULT = 0.1   # equity < 10% del capital base -> se considera corrupto
EQUITY_SANITY_MAX_MULT = 10.0  # equity > 10x el capital base -> se considera corrupto

# Filtro histórico por tipo de zona (ver src/historical_analyzer.py y
# src/zone_performance_db.py) - decide() lo consulta como confluencia
# adicional, nunca como único criterio. IMPORTANTE: estos umbrales son
# relativos a LA METODOLOGÍA de ese backtest (TP y SL ambos escalados por
# ATR con un ratio fijo), no a retornos esperados en trading real - con esa
# metodología el R-múltiple de una victoria queda acotado a ~1.0, así que un
# umbral tipo "avg_r > 1.2" nunca se cumpliría sin importar qué tan buena
# sea la zona. Ajustados a lo que ese backtest puede medir de verdad.
ZONE_HIST_MIN_TESTS = 10   # sin esto, "sin datos" nunca es lo mismo que "zona mala"
ZONE_HIST_REJECT_WR = 0.40  # win rate histórico por debajo de esto -> rechazo duro
ZONE_HIST_BOOST1_WR = 0.55  # win rate por encima de esto -> conviction +1
ZONE_HIST_BOOST2_WR = 0.60  # win rate por encima de esto (y avg_r>0) -> conviction +2

# Paper Trading Engine: fricción realista (comisión + slippage adverso), y
# saneamiento de señal para que un dato HTF corrupto puntual (ver
# orderflow_data.volume_profile) nunca llegue a emitirse como señal.
PAPER_FEE_RATE = 0.0004        # 0.04% por lado (entrada y salida)
PAPER_SLIPPAGE_PCT = 0.0005    # 0.05% adverso sobre el precio de fill
SIGNAL_MIN_TP_DIST_USDT = 20.0  # TP1 a menos de esto de distancia -> señal inválida
SIGNAL_MIN_SL_DIST_USDT = 5.0   # SL a menos de esto de distancia -> señal inválida
TP2_MAX_MULT_OF_TP1 = 5.0       # TP2 a más de 5x la distancia de TP1 -> objetivo no creíble
HTF_LEVEL_SANITY_BAND_PCT = 0.20  # nivel HTF (POC/VAH/VAL) a más de 20% del precio -> se descarta como corrupto
