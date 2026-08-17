"""Valida HTFFundingBrain (motor swing REAL, src/brain_htf_funding.py) sobre
datos BTCUSDT recientes (ultimos N meses, Binance SPOT) que el motor NUNCA
vio -- el walk-forward original (backtest_htf.py) uso ene-nov 2025. Esto es
una verificacion out-of-time genuina, no un re-ajuste de parametros: se
reusa el pipeline COMPLETO de backtest_htf.py sin tocar nada (calcular_htf/
calcular_features_1m/agregar_initiative_pullback/simular_ventana/
calcular_metricas), solo se cambia la fuente de datos.

NO es un walk-forward con train/test como el original (serian 6 meses
partidos en ventanas demasiado chicas para tener sentido) -- es UNA sola
ventana de test sobre todo el periodo descargado, tratado como out-of-time
puro. Costos: FUNDEDNEXT_PROP (mismo perfil ya usado y validado). DD:
se reporta tanto el trailing peak-to-trough (metrica nativa de
calcular_metricas) como el DD estatico desde balance inicial (el que
realmente usan las reglas de FundedNext -- ver conversacion previa, misma
metodologia que ya se aplico para comparar contra las reglas reales).

Uso:
    python scripts/run_backtest_swing_recent.py --months 6
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

import backtest_htf as bth  # noqa: E402 -- reusa el pipeline real tal cual, sin tocarlo

DATA_PATH = REPO_ROOT / "data" / "btcusdt_spot_1m_recent.parquet"
RESULTS_DIR = REPO_ROOT / "results"
COST_MODEL = "FUNDEDNEXT_PROP"  # confirmado por el usuario: mismas reglas ya validadas (5% diario / 10% estatico)


def load_recent_1m() -> pd.DataFrame:
    """Misma forma que ofd.load_klines() (via bth.cargar_1m para los CSV
    viejos) -- el parquet ya tiene las 12 columnas de klines (ver
    download_btc_spot_recent.py), solo falta delta/cvd."""
    if not DATA_PATH.exists():
        raise SystemExit(f"No existe {DATA_PATH} -- correr primero scripts/download_btc_spot_recent.py")
    df = pd.read_parquet(DATA_PATH)
    df = df.sort_values("open_time").drop_duplicates("open_time").reset_index(drop=True)
    taker_sell_vol = df["volume"] - df["taker_buy_base_vol"]
    df["delta"] = df["taker_buy_base_vol"] - taker_sell_vol
    df["cvd"] = df["delta"].cumsum()
    return df


def run(cost_model: str, capital_base: float = 50_000.0) -> dict:
    print(f"Cargando {DATA_PATH.name}...")
    t0 = time.perf_counter()
    df_1m_raw = load_recent_1m()
    print(f"  {len(df_1m_raw)} velas 1m | {df_1m_raw['open_time'].min()} -> {df_1m_raw['open_time'].max()} ({time.perf_counter()-t0:.1f}s)")

    asset_cfg = bth._apply_cost_overlay(bth.get_asset_config("BTCUSDT"), cost_model)
    params = bth.HTFParams()

    t0 = time.perf_counter()
    df_15m_raw = bth.ofd.resample_klines(df_1m_raw, "15min")
    df_1h_raw = bth.ofd.resample_klines(df_1m_raw, "1h")
    df_15m = bth.calcular_htf(df_1h_raw, df_15m_raw)
    df_1m = bth.calcular_features_1m(df_1m_raw, params)
    df_1m = pd.merge_asof(
        df_1m.sort_values("open_time"),
        df_15m[["open_time", "poc", "vah", "val", "swing_h4_h", "swing_h4_l", "vwap_15m", "cvd_trend", "atr14_15m", "atr_rank_30d"]].sort_values("open_time"),
        on="open_time", direction="backward",
    )
    df_1m = bth.agregar_initiative_pullback(df_1m, params)
    print(f"Precomputo total: {time.perf_counter() - t0:.1f}s")

    fecha_min, fecha_max = df_1m["open_time"].min(), df_1m["open_time"].max()
    warmup_dias = 20  # POC/VAH/VAL/swing H4/ATR necesitan historia real antes del primer trade -- menor que los 60 dias del WF original porque acá es UNA sola ventana, no 3, y 6 meses totales no sobran margen
    test_start = fecha_min + pd.Timedelta(days=warmup_dias)
    df_test = df_1m[df_1m["open_time"] >= test_start].reset_index(drop=True)
    dias_test = (fecha_max - test_start).total_seconds() / 86400

    print(f"\nVentana de test (out-of-time, un solo pase): {test_start.date()} -> {fecha_max.date()} ({dias_test:.0f} dias, tras {warmup_dias}d de warmup)")
    t0 = time.perf_counter()
    trades = bth.simular_ventana(df_test, params, asset_cfg)
    print(f"Simulacion completa en {time.perf_counter()-t0:.1f}s -- {len(trades)} trades")

    metricas = bth.calcular_metricas(trades, dias_test, capital_base=capital_base)

    # DD estatico desde balance inicial (la metrica que realmente usan las
    # reglas de FundedNext -- ver conversacion previa) ademas del trailing
    # peak-to-trough nativo de calcular_metricas. PF/WR/avg_r/DD% son
    # invariantes al capital (R-multiplos, capital_base se cancela en la
    # formula) -- solo el $ de ingreso mensual escala linealmente, ver
    # docstring del modulo.
    risk_pct_trade = 0.005
    if trades:
        r_multiples = np.array([t["pnl_r"] for t in trades])
        pnl_usdt = r_multiples * risk_pct_trade * capital_base
        equity = capital_base + np.cumsum(pnl_usdt)
        static_dd_pct = float((capital_base - equity.min()) / capital_base * 100) if equity.min() < capital_base else 0.0
        monthly_income_usd = float(metricas["trades_per_month"] * metricas["avg_r"] * risk_pct_trade * capital_base)
    else:
        static_dd_pct = 0.0
        monthly_income_usd = 0.0

    return {
        "metricas": metricas, "static_dd_pct": round(static_dd_pct, 2),
        "monthly_income_usd": round(monthly_income_usd, 2), "capital_base": capital_base,
        "n_trades": len(trades), "test_start": str(test_start.date()), "test_end": str(fecha_max.date()),
        "dias_test": round(dias_test, 1),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Verifica HTFFundingBrain (swing) sobre datos BTC recientes, out-of-time")
    parser.add_argument("--cost-model", type=str, default=COST_MODEL, choices=list(bth.BROKER_COST_OVERLAYS.keys()))
    parser.add_argument("--capital", type=float, default=50_000.0)
    args = parser.parse_args()

    result = run(args.cost_model, capital_base=args.capital)
    m = result["metricas"]

    print(f"\n=== RESULTADO OUT-OF-TIME (swing, datos nunca vistos por el motor, capital ${args.capital:,.0f}) ===")
    print(f"  periodo: {result['test_start']} -> {result['test_end']} ({result['dias_test']:.0f} dias)")
    print(f"  trades: {m['total_trades']}")
    print(f"  win_rate: {m['win_rate']:.1%}")
    print(f"  profit_factor: {m['profit_factor']}")
    print(f"  avg_r: {m['avg_r']}")
    print(f"  trailing_max_dd_pct: {m['max_dd_pct']:.2%}")
    print(f"  static_dd_pct (regla real FundedNext): {result['static_dd_pct']:.2f}%")
    print(f"  trades_per_month: {m['trades_per_month']}")
    print(f"  proyeccion ingreso mensual @ ${args.capital:,.0f}: ${result['monthly_income_usd']:,.2f}")

    baseline_pf = 2.6549  # walk-forward original ene-nov 2025, FUNDEDNEXT_PROP -- ver results/btcusdt_htf_wf_fundednext_prop_20260722_223222.json
    report = {
        "strategy": "HTFFundingBrain (swing)", "cost_model": args.cost_model, "capital_base": args.capital,
        "period": {"start": result["test_start"], "end": result["test_end"], "days": result["dias_test"]},
        "note": "Ventana UNICA out-of-time (no walk-forward con train/test) -- verifica el motor ya validado sobre datos que nunca vio, no re-ajusta nada. PF/WR/avg_r/DD%% son invariantes al capital -- solo el ingreso mensual en USD escala.",
        "metrics": m,
        "static_dd_pct": result["static_dd_pct"],
        "monthly_income_usd": result["monthly_income_usd"],
        "baseline_pf_original_wf": baseline_pf,
        "supera_baseline": (m["profit_factor"] > baseline_pf) if m["total_trades"] > 0 else None,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    RESULTS_DIR.mkdir(exist_ok=True)
    out_path = RESULTS_DIR / f"swing_btc_recent_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    out_path.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    print(f"\nReporte escrito en {out_path}")


if __name__ == "__main__":
    main()
