"""Cuantifica el riesgo de gap de fin de semana para el motor swing en un
MT5 de prop firm (FundedNext) que se congela viernes-domingo, algo que el
backtest original NO modela (opera sobre datos continuos 24/7 de Binance
spot, sin ventanas de mercado cerrado).

Confirmado leyendo backtest_htf.py: simular_ventana() cierra SL/TP siempre
al precio EXACTO declarado (pos["sl_price"]), nunca al precio real de la
vela que lo dispara -- asume fills perfectos sin gap, valido en cripto
continuo pero NO en MT5 con mercado cerrado.

SUPUESTO A VERIFICAR: ventana de cierre viernes 21:00 UTC -> domingo 21:00
UTC (convencion estandar forex/CFD, no confirmada especificamente para el
feed cripto de FundedNext -- si su horario real difiere, este numero
cambia).

Hace DOS cosas:
1. Cuantifica cuantos trades reales quedaron abiertos durante un fin de
   semana, y si el precio REAL de Binance durante ese fin de semana se
   movio mas alla del SL declarado (gap real que el backtest no capturo).
2. Corre una variante "Friday flat" (se excluyen las velas de fin de
   semana del dataset, forzando que ninguna posicion pueda gestionarse
   durante ese periodo -- aproximacion a "el bot cierra todo el viernes")
   y compara PF/WR/DD real contra la version original.

Uso:
    python scripts/eval_weekend_gap_risk.py
"""
from __future__ import annotations

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

COST_MODEL = "FUNDEDNEXT_PROP"
WARMUP_DIAS = 20


def _is_weekend_closed(ts: pd.Timestamp) -> bool:
    """Viernes 21:00 UTC -> domingo 21:00 UTC (convencion forex/CFD
    estandar). Weekday: lunes=0 ... domingo=6."""
    wd, h = ts.weekday(), ts.hour
    if wd == 4 and h >= 21:  # viernes desde las 21:00
        return True
    if wd == 5:  # sabado entero
        return True
    if wd == 6 and h < 21:  # domingo hasta las 21:00
        return True
    return False


def precompute_full(df_1m_raw: pd.DataFrame) -> pd.DataFrame:
    asset_cfg = bth._apply_cost_overlay(bth.get_asset_config("BTCUSDT"), COST_MODEL)
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
    return df_1m, asset_cfg, params


def analizar_exposicion_weekend(trades: list, df_1m_raw: pd.DataFrame) -> None:
    df_idx = df_1m_raw.set_index("open_time")
    afectados = []

    for t in trades:
        entry_ts, exit_ts = t["entry_ts"], t["exit_ts"]
        # hay algun momento "cerrado" entre entry y exit?
        span = pd.date_range(entry_ts, exit_ts, freq="h")
        cruza_weekend = any(_is_weekend_closed(ts) for ts in span)
        if not cruza_weekend:
            continue

        ventana = df_1m_raw[(df_1m_raw["open_time"] >= entry_ts) & (df_1m_raw["open_time"] <= exit_ts)]
        ventana_cerrada = ventana[ventana["open_time"].apply(_is_weekend_closed)]
        if ventana_cerrada.empty:
            continue

        peor_low = ventana_cerrada["low"].min()
        peor_high = ventana_cerrada["high"].max()
        direccion = t["direccion"]
        sl_price = None
        # el SL final registrado en el trade puede ser el de BE tras TP1 --
        # usamos el peor caso conocido en el trade (sl_price no se guarda
        # historico, aproximamos con riesgo_original desde entry)
        entry_price = t.get("entry_price")
        riesgo = t.get("riesgo_original")
        if entry_price is None or riesgo is None:
            continue
        sl_price = entry_price - riesgo if direccion == "LONG" else entry_price + riesgo

        gap_breach = (peor_low <= sl_price) if direccion == "LONG" else (peor_high >= sl_price)
        afectados.append({
            "entry_ts": entry_ts, "exit_ts": exit_ts, "direccion": direccion,
            "sl_price": round(sl_price, 1), "peor_precio_weekend": round(peor_low if direccion == "LONG" else peor_high, 1),
            "gap_breach": gap_breach,
        })

    print(f"\n=== EXPOSICION A FIN DE SEMANA (de {len(trades)} trades reales) ===")
    print(f"  Trades abiertos durante un cierre de mercado (viernes 21:00 - domingo 21:00 UTC): {len(afectados)}")
    breaches = [a for a in afectados if a["gap_breach"]]
    print(f"  De esos, con precio real de Binance mas alla del SL declarado durante el cierre: {len(breaches)}")
    if breaches:
        print("  Esto significa: en MT5 real, esos trades se hubieran cerrado con perdida MAYOR a la modelada")
        print("  (el backtest los cerro al SL exacto, pero el precio real paso ese nivel durante el fin de semana):")
        for b in breaches[:10]:
            print(f"    {b['entry_ts']} -> {b['exit_ts']} | {b['direccion']} | SL={b['sl_price']} | peor precio real={b['peor_precio_weekend']}")


def run_variante_friday_flat(df_1m_raw: pd.DataFrame) -> dict:
    print("\nCorriendo variante 'Friday flat' (velas de fin de semana excluidas del dataset)...")
    df_sin_weekend = df_1m_raw[~df_1m_raw["open_time"].apply(_is_weekend_closed)].reset_index(drop=True)
    print(f"  {len(df_1m_raw)} velas originales -> {len(df_sin_weekend)} sin fin de semana ({len(df_1m_raw)-len(df_sin_weekend)} removidas)")

    df_1m, asset_cfg, params = precompute_full(df_sin_weekend)
    test_start = df_1m["open_time"].min() + pd.Timedelta(days=WARMUP_DIAS)
    df_test = df_1m[df_1m["open_time"] >= test_start].reset_index(drop=True)

    t0 = time.perf_counter()
    trades = bth.simular_ventana(df_test, params, asset_cfg)
    print(f"  {len(trades)} trades | sim en {time.perf_counter()-t0:.1f}s")

    dias_test = (df_1m["open_time"].max() - test_start).total_seconds() / 86400
    return bth.calcular_metricas(trades, dias_test), trades


def main() -> None:
    print("Cargando datos...")
    df_1m_raw = load_recent_1m()

    print("Corriendo pipeline completo (con fin de semana, version YA validada)...")
    df_1m_full, asset_cfg, params = precompute_full(df_1m_raw)
    test_start = df_1m_full["open_time"].min() + pd.Timedelta(days=WARMUP_DIAS)
    df_test_full = df_1m_full[df_1m_full["open_time"] >= test_start].reset_index(drop=True)
    trades_full = bth.simular_ventana(df_test_full, params, asset_cfg)
    dias_full = (df_1m_full["open_time"].max() - test_start).total_seconds() / 86400
    metricas_full = bth.calcular_metricas(trades_full, dias_full)

    analizar_exposicion_weekend(trades_full, df_1m_raw)

    metricas_flat, trades_flat = run_variante_friday_flat(df_1m_raw)

    print("\n=== COMPARACION: con fin de semana (ya validado) vs Friday-flat (conservador) ===")
    print(f"{'Metrica':<20} {'Con weekend':>15} {'Friday-flat':>15}")
    for k in ("total_trades", "win_rate", "profit_factor", "avg_r", "max_dd_pct", "trades_per_month"):
        v1, v2 = metricas_full.get(k), metricas_flat.get(k)
        print(f"{k:<20} {v1!s:>15} {v2!s:>15}")


if __name__ == "__main__":
    main()
