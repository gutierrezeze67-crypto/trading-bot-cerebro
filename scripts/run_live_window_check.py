"""Corre el backtest real (misma logica que produccion, ver
backtest_htf.py) sobre la ventana de trading en vivo reciente
(2026-08-04 -> ahora), usando datos reales de Binance recien descargados
(fetch_recent_klines.py) porque el dataset local solo llega a nov-2025.

Objetivo: saber que hubiera pasado con las 26+ señales de swing que el
motor encontro en vivo y no pudo ejecutar (bug de redondeo de volumen,
ya arreglado) -- no reconstruye cada señal puntual desde los logs (el
precio de entrada/SL/TP no quedo logueado), sino que corre la MISMA
logica determinista contra el mismo período real de mercado."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

from src import orderflow_data as ofd
from src.brain_htf_funding import HTFParams
from config.assets import get_asset_config

import backtest_htf as bt

CSV_PATH = Path(__file__).resolve().parent / "BTCUSDT-1m-2026-07-08.csv"
TEST_START = pd.Timestamp("2026-08-04")
TEST_END = pd.Timestamp("2026-08-15")


def main() -> None:
    asset_cfg = bt._apply_cost_overlay(get_asset_config("BTCUSDT"), "VANTAGE_RAW_ECN")

    print(f"Cargando {CSV_PATH.name}...")
    df_1m_raw = ofd.load_klines(CSV_PATH)
    print(f"  {len(df_1m_raw)} velas 1m | {df_1m_raw['open_time'].min()} -> {df_1m_raw['open_time'].max()}")

    df_15m_raw = ofd.resample_klines(df_1m_raw, "15min")
    df_1h_raw = ofd.resample_klines(df_1m_raw, "1h")

    params = HTFParams()
    df_15m = bt.calcular_htf(df_1h_raw, df_15m_raw)

    df_1m = bt.calcular_features_1m(df_1m_raw, params)
    df_1m = pd.merge_asof(
        df_1m.sort_values("open_time"),
        df_15m[["open_time", "poc", "vah", "val", "swing_h4_h", "swing_h4_l", "vwap_15m", "cvd_trend", "atr14_15m", "atr_rank_30d"]].sort_values("open_time"),
        on="open_time", direction="backward",
    )
    df_1m = bt.agregar_initiative_pullback(df_1m, params)

    df_ventana = df_1m[(df_1m["open_time"] >= TEST_START) & (df_1m["open_time"] < TEST_END)].reset_index(drop=True)
    print(f"\nVentana de test real: {TEST_START.date()} -> {TEST_END.date()} ({len(df_ventana)} velas 1m)")

    trades = bt.simular_ventana(df_ventana, params, asset_cfg)
    dias = (TEST_END - TEST_START).total_seconds() / 86400
    metricas = bt.calcular_metricas(trades, dias)

    print("\n" + "=" * 60)
    print(f"=== RESULTADO REAL: {TEST_START.date()} -> {TEST_END.date()} (cost_model=VANTAGE_RAW_ECN) ===")
    print(f"Trades: {metricas['total_trades']}")
    print(f"Win Rate: {metricas['win_rate']:.1%}")
    print(f"Profit Factor: {metricas['profit_factor']:.2f}")
    print(f"Max DD: {metricas['max_dd_pct']:.2%}")
    print(f"Trades/mes (proyectado): {metricas['trades_per_month']:.1f}")
    print("=" * 60)

    print("\n--- Detalle de cada trade ---")
    for t in trades:
        print(t)


if __name__ == "__main__":
    main()
