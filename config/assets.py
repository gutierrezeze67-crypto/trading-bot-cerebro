"""
Config de costos reales por activo, usada por src/brain_htf_funding.py y
backtest_htf.py. Solo BTCUSDT tiene datos reales en este repo (ver
backtest_htf.py: 11 CSV mensuales de Binance spot, ene-nov 2025) - no se
agregan otros símbolos hasta tener datos reales para ellos (nada de
XAUUSD/US30/NAS100 acá pese a que esos archivos existen en el disco, porque
ninguno trae volumen comprador/vendedor - sin eso no hay delta/CVD, y sin
delta/CVD la mayoría de los detectores del cerebro no son calculables, no
solo "menos precisos").
"""
ASSET_CONFIG = {
    "BTCUSDT": {
        "venue": "BINANCE_FUT", "asset_class": "crypto_fut",
        "atr_mult": 1.0, "session": "24/7", "timezone": "UTC",
        "spread_bps": 1.0, "fee_bps": 4.0, "slippage_bps": 0.5,
        "swap_long_bps_8h": -2.0, "swap_short_bps_8h": 1.0,
        "contract_size": 1, "tick_size": 0.10, "tick_value": 0.10,
        "margin_mode": "isolated", "max_leverage": 20,
        "funding_interval_h": 8, "min_tp1_spread_mult": 3.0,
    },
}


def get_asset_config(symbol: str) -> dict:
    if symbol not in ASSET_CONFIG:
        raise ValueError(f"Asset {symbol} no configurado (sin datos reales). Disponibles: {list(ASSET_CONFIG.keys())}")
    return ASSET_CONFIG[symbol].copy()


# Overlays de costo por broker/cuenta -- antes vivian solo en backtest_htf.py
# (a proposito, para "no tocar el archivo compartido con produccion") pero
# eso significaba que HTFFundingBrain en vivo (unified_brain/main_orchestrator.py)
# nunca los aplicaba: siempre usaba el ASSET_CONFIG de arriba tal cual, que
# es el perfil MAS BARATO (Binance Futures institucional, spread 1.0bps) --
# ni Vantage (cuenta real que ejecuta hoy) ni FundedNext/FundingPips (destino
# real de la plataforma) tienen ese costo. Validado 2026-08-16: con el filtro
# de costo FUNDEDNEXT_PROP (mas estricto), la ventana mala del 4-15 ago 2026
# pasa de 34 trades/PF 1.31 a solo 2 trades -- el costo mas realista actua
# como filtro de calidad de señal, no solo de contabilidad de PnL. Ver
# results/ y conversacion 2026-08-16 para el detalle completo.
BROKER_COST_OVERLAYS = {
    "BINANCE_FUTURES_CURRENT": {},  # sin overlay - usa asset_cfg tal cual (spread_bps=1.0, fee_bps=4.0 PCT_NOTIONAL, slippage_bps=0.5)
    "VANTAGE_RAW_ECN": {
        # Mismos supuestos que backtest_standard_account.py::COST_PROFILES
        # (ver ese archivo para las fuentes citadas: comision Raw ECN $3/lado
        # publicada, sin dato cripto-especifico confirmado -> monto FIJO en
        # USD, no bps). Spread algo mas ajustado que el baseline Binance
        # (supuesto, Raw ECN institucional vs futuros retail).
        "spread_bps": 0.8, "slippage_bps": 0.5,
        "commission_mode": "FIXED_USD", "commission_fixed_usdt_per_side": 3.0, "fee_bps": 0.0,
    },
    "FUNDEDNEXT_PROP": {
        # Comision cripto oficial FundedNext: 0.04%/lado = 4bps/lado (mapea
        # directo al campo fee_bps existente, PCT_NOTIONAL). Spread ancho
        # ($20-80 en BTCUSD publicado, sin bps fijo) -> 6.7bps de base
        # (mismo criterio que el perfil scalping equivalente). Slippage algo
        # mayor que el baseline (prop firm, no ECN directo).
        "spread_bps": 6.7, "slippage_bps": 1.0,
        "commission_mode": "PCT_NOTIONAL", "fee_bps": 4.0,
    },
}


def apply_cost_overlay(asset_cfg: dict, cost_model: str) -> dict:
    cfg = asset_cfg.copy()
    cfg.setdefault("commission_mode", "PCT_NOTIONAL")
    cfg.setdefault("commission_fixed_usdt_per_side", 0.0)
    cfg["cost_model"] = cost_model
    cfg.update(BROKER_COST_OVERLAYS[cost_model])
    return cfg
