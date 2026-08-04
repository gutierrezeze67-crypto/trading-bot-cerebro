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
