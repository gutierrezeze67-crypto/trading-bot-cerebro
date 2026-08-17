"""Evalua challenges de FundedNext usando la SECUENCIA REAL de trades del
backtest out-of-time de swing (scripts/run_backtest_swing_recent.py,
feb-jul 2026, datos que el motor nunca vio) -- no un promedio teorico, sino
un replay dia por dia chequeando las reglas de DD en cada punto.

Mecanica confirmada por el usuario (fuentes: fundednext.com, fxprop.com,
track360.io) y YA correcta en este script sin necesidad de cambios:
- Daily Loss Limit BALANCE-BASED (no equity flotante): el equity solo se
  actualiza en este script cuando un trade CIERRA (pnl_r aplicado en
  exit_ts), nunca por marca-a-mercado de una posicion swing todavia
  abierta -- una perdida flotante mientras el trade sigue vivo no cuenta
  contra el limite diario hasta que se cierra, que es exactamente como
  FundedNext lo calcula. No hace falta modelar equity intradia/flotante.
- Maximum Loss Limit STATIC: medido desde el balance INICIAL fijo (nunca
  se mueve con las ganancias) -- ya era el criterio usado.

Uso:
    python scripts/eval_fundednext_challenge.py --capital 50000 --dispersion
"""
from __future__ import annotations

import argparse
import sys
import time
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

import backtest_htf as bth  # noqa: E402
from run_backtest_swing_recent import load_recent_1m  # noqa: E402

RISK_PCT_TRADE = 0.005

RULES_1STEP = {
    "name": "FundedNext 1-Step $50k",
    "profit_target_pct": 0.10, "daily_loss_pct": 0.03, "max_loss_pct": 0.06, "min_trading_days": 2,
}
RULES_FUNDINGPIPS_FLEX = {
    "name": "FundingPips 1-Step Flex $50k",
    "profit_target_pct": 0.12, "daily_loss_pct": 0.03, "max_loss_pct": 0.12, "min_trading_days": 0,
}


def get_trades(cost_model: str = "FUNDEDNEXT_PROP") -> list:
    print("Cargando datos y corriendo pipeline real (mismo que run_backtest_swing_recent.py)...")
    t0 = time.perf_counter()
    df_1m_raw = load_recent_1m()
    asset_cfg = bth._apply_cost_overlay(bth.get_asset_config("BTCUSDT"), cost_model)
    params = bth.HTFParams()

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

    warmup_dias = 20
    test_start = df_1m["open_time"].min() + pd.Timedelta(days=warmup_dias)
    df_test = df_1m[df_1m["open_time"] >= test_start].reset_index(drop=True)

    trades = bth.simular_ventana(df_test, params, asset_cfg)
    print(f"  {len(trades)} trades reales | precomputo+sim en {time.perf_counter()-t0:.1f}s")
    return sorted(trades, key=lambda t: t["exit_ts"])


def _simulate_from(trades_sorted: list, capital: float, rules: dict, risk_pct_trade: float = RISK_PCT_TRADE) -> dict:
    """Corre la simulacion de un challenge arrancando en el PRIMER trade de
    trades_sorted (se le pasa un slice ya recortado para simular "empezar
    el challenge en otro punto del historico"). Devuelve el resultado sin
    imprimir nada -- para reusar tanto en el caso unico como en la
    dispersion multi-arranque."""
    daily_limit_usd = capital * rules["daily_loss_pct"]
    max_loss_usd = capital * rules["max_loss_pct"]
    target_usd = capital * rules["profit_target_pct"]

    equity = capital
    day_start_equity = capital
    current_day = None
    trading_days = set()

    for t in trades_sorted:
        day = t["exit_ts"].date()
        if day != current_day:
            day_start_equity = equity
            current_day = day
        trading_days.add(day)

        pnl_usd = t["pnl_r"] * risk_pct_trade * capital
        equity += pnl_usd

        daily_dd = day_start_equity - equity
        static_dd = capital - equity

        if daily_dd >= daily_limit_usd:
            return {"outcome": "BREACH", "type": "DAILY_LOSS", "date": day, "equity": round(equity, 2)}
        if static_dd >= max_loss_usd:
            return {"outcome": "BREACH", "type": "MAX_LOSS_STATIC", "date": day, "equity": round(equity, 2)}

        if equity - capital >= target_usd and len(trading_days) >= rules["min_trading_days"]:
            days = (day - trades_sorted[0]["exit_ts"].date()).days + 1
            return {"outcome": "PASS", "days": days, "date": day, "equity": round(equity, 2)}

    return {"outcome": "NO_RESOLUTION", "final_equity": round(equity, 2)}


def evaluar_challenge(trades_sorted: list, capital: float, rules: dict) -> None:
    r = _simulate_from(trades_sorted, capital, rules)
    print(f"\n=== EVALUACION CHALLENGE FUNDEDNEXT {rules['name']} ${capital:,.0f} ===")
    print(f"  Target: +{rules['profit_target_pct']:.0%} | Daily loss: {rules['daily_loss_pct']:.0%} | Max loss static: {rules['max_loss_pct']:.0%} | Min dias: {rules['min_trading_days']}")

    if r["outcome"] == "BREACH":
        print(f"\n  RESULTADO: CUENTA QUEMADA -- {r['type']} el {r['date']} (equity ${r['equity']:,.2f})")
    elif r["outcome"] == "PASS":
        print(f"\n  RESULTADO: PASA el {r['date']} -- {r['days']} dias corridos desde el primer trade (equity ${r['equity']:,.2f})")
    else:
        print(f"\n  RESULTADO: no llega al target en el periodo evaluado (equity final ${r['final_equity']:,.2f})")


def dispersion(trades_sorted: list, capital: float, rules: dict) -> None:
    """Corre el mismo challenge arrancando en CADA dia distinto que tiene
    al menos un trade en el historico real (no un promedio sintetico --
    cada arranque es un subconjunto real y contiguo de la secuencia real de
    trades). Reporta la distribucion de resultados."""
    dias_unicos = sorted({t["exit_ts"].date() for t in trades_sorted})
    print(f"\nCorriendo dispersion: {len(dias_unicos)} puntos de arranque posibles (un dia real distinto cada uno)...")

    resultados = []
    for dia_inicio in dias_unicos:
        subset = [t for t in trades_sorted if t["exit_ts"].date() >= dia_inicio]
        if not subset:
            continue
        r = _simulate_from(subset, capital, rules)
        resultados.append(r)

    passes = [r for r in resultados if r["outcome"] == "PASS"]
    breaches = [r for r in resultados if r["outcome"] == "BREACH"]
    no_res = [r for r in resultados if r["outcome"] == "NO_RESOLUTION"]

    print(f"\n=== DISPERSION -- {len(resultados)} arranques distintos, challenge {rules['name']} ${capital:,.0f} ===")
    print(f"  PASA:            {len(passes)}/{len(resultados)} ({len(passes)/len(resultados):.0%})")
    print(f"  QUEMADA:         {len(breaches)}/{len(resultados)} ({len(breaches)/len(resultados):.0%})")
    print(f"  SIN RESOLVER:    {len(no_res)}/{len(resultados)} ({len(no_res)/len(resultados):.0%}, se acabo el historico antes de pasar o quemarse)")

    if passes:
        dias = np.array([r["days"] for r in passes])
        print(f"\n  Dias para pasar (solo los que pasaron): min={dias.min()} p25={np.percentile(dias,25):.0f} "
              f"mediana={np.median(dias):.0f} p75={np.percentile(dias,75):.0f} max={dias.max()}")
    if breaches:
        tipos = {}
        for r in breaches:
            tipos[r["type"]] = tipos.get(r["type"], 0) + 1
        print(f"  Causas de quema: {tipos}")


RULESETS = {"fundednext": RULES_1STEP, "fundingpips": RULES_FUNDINGPIPS_FLEX}


def main() -> None:
    parser = argparse.ArgumentParser(description="Evalua challenges con la secuencia real de trades")
    parser.add_argument("--capital", type=float, default=50_000.0)
    parser.add_argument("--cost-model", type=str, default="FUNDEDNEXT_PROP",
                         help="Costos reales del trade (comision/spread) -- seguimos usando el perfil de FundedNext porque "
                              "es el unico sourced para cripto; las reglas de pass/fail de FundingPips se aplican aparte, "
                              "sin costos FundingPips-especificos verificados todavia.")
    parser.add_argument("--rules", type=str, default="fundednext", choices=list(RULESETS.keys()))
    parser.add_argument("--dispersion", action="store_true", help="corre desde cada dia distinto del historico, no solo el primero")
    args = parser.parse_args()

    trades = get_trades(args.cost_model)
    if not trades:
        raise SystemExit("0 trades -- no se puede evaluar el challenge.")

    rules = RULESETS[args.rules]
    evaluar_challenge(trades, args.capital, rules)
    if args.dispersion:
        dispersion(trades, args.capital, rules)


if __name__ == "__main__":
    main()
