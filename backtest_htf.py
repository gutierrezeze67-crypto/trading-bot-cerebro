"""
Backtest walk-forward del cerebro HTF de fondeo (src/brain_htf_funding.py).
Estrategia separada del cerebro principal (src/order_flow_signal.py) - no lo
toca ni depende de él.

DATOS: 11 CSV mensuales reales de Binance (spot, formato oficial
binance-public-data) en C:\\Users\\ezequiel\\Desktop\\bitcoin_data\\spot\\monthly\\
klines\\BTCUSDT\\1m\\BTCUSDT-1m-2025-{01..11}.csv - enero a noviembre 2025,
~11 meses reales sin huecos. El HTF (15m/1h) se resamplea del mismo 1m (no
se mezcla con los CSV futures del cerebro principal, que además no cubren
el mismo rango de fechas - una sola fuente evita cualquier discrepancia
spot/futures).

ZERO LOOK-AHEAD: todo el contexto HTF (POC/VAH/VAL/swing H4/VWAP/CVD
trend/ATR) se calcula con pandas.merge_asof(direction="backward") - en la
barra 1m de timestamp T solo se ve HTF de velas 15m/1h/4h YA CERRADAS antes
de T, nunca datos futuros.

Uso:
    python backtest_htf.py --wf 3
"""
import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parent))

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass

from src import orderflow_data as ofd
from src.brain_htf_funding import HTFFundingBrain, HTFParams
from config.assets import get_asset_config, BROKER_COST_OVERLAYS, apply_cost_overlay as _apply_cost_overlay

DATA_DIR_1M = Path(r"C:\Users\ezequiel\Desktop\bitcoin_data\spot\monthly\klines\BTCUSDT\1m")
RESULTS_DIR = Path(__file__).resolve().parent / "results"

ROLLING_1H_WINDOW = 200   # ~8 dias, mismo criterio que historical_analyzer.py
SWING_4H_LOOKBACK = 96    # ~16 dias
ATR_RANK_LOOKBACK_BARS = 2880  # 30 dias de velas 15m (96/dia) -- ver atr_compression_percentile

PASS_TARGETS = {
    "win_rate": 0.45,
    "profit_factor": 2.2,
    "max_dd_pct": 0.05,          # se evalua como <= target
    "trades_per_month_min": 60.0,
    "trades_per_month_max": 120.0,
}

# BROKER_COST_OVERLAYS / _apply_cost_overlay ahora viven en config/assets.py
# (compartido con unified_brain en produccion, ver docstring ahi) -- antes
# estaban solo aca "para no tocar el archivo compartido", pero eso era
# justamente el problema: el motor en vivo nunca podia aplicar ningun
# overlay real de broker. Se importan arriba con el mismo nombre para no
# tocar el resto de este archivo ni los scripts que ya hacen
# `bth.BROKER_COST_OVERLAYS` / `bth._apply_cost_overlay`.


# ----------------------------------------------------------------------
# Carga y resample
# ----------------------------------------------------------------------

def cargar_1m() -> pd.DataFrame:
    archivos = sorted(DATA_DIR_1M.glob("BTCUSDT-1m-2025-*.csv"))
    if not archivos:
        print(f"❌ No se encontraron CSV en {DATA_DIR_1M}")
        sys.exit(1)
    dfs = [ofd.load_klines(f) for f in archivos]
    df = pd.concat(dfs, ignore_index=True).sort_values("open_time").drop_duplicates("open_time").reset_index(drop=True)
    return df


def _atr(df: pd.DataFrame, periods: int) -> pd.Series:
    prev_close = df["close"].shift(1).bfill()
    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - prev_close).abs(),
        (df["low"] - prev_close).abs(),
    ], axis=1).max(axis=1)
    return tr.rolling(periods, min_periods=1).mean()


# ----------------------------------------------------------------------
# Contexto HTF (POC/VAH/VAL rolling 1h, swing H4, VWAP15m, CVD trend, ATR)
# ----------------------------------------------------------------------

def calcular_htf(df_1h: pd.DataFrame, df_15m: pd.DataFrame) -> pd.DataFrame:
    print("🧮 Calculando POC/VAH/VAL rolling (200 velas 1h)...")
    poc, vah, val = [np.nan] * len(df_1h), [np.nan] * len(df_1h), [np.nan] * len(df_1h)
    for i in range(ROLLING_1H_WINDOW, len(df_1h)):
        vp = ofd.volume_profile(df_1h.iloc[i - ROLLING_1H_WINDOW:i])
        poc[i], vah[i], val[i] = vp["poc"], vp["vah"], vp["val"]
    df_1h = df_1h.copy()
    df_1h["poc"], df_1h["vah"], df_1h["val"] = poc, vah, val

    print("🧮 Calculando swing H4 rolling (96 velas de 4h)...")
    df_4h = ofd.resample_klines(df_1h[["open_time", "open", "high", "low", "close", "volume", "close_time",
                                        "quote_volume", "num_trades", "taker_buy_base_vol", "taker_buy_quote_vol"]], "4h")
    df_4h["swing_h4_h"] = df_4h["high"].rolling(SWING_4H_LOOKBACK, min_periods=20).max()
    df_4h["swing_h4_l"] = df_4h["low"].rolling(SWING_4H_LOOKBACK, min_periods=20).min()

    print("🧮 Calculando VWAP15m/CVD-trend15m/ATR14-15m (vectorizado)...")
    df_15m = df_15m.copy()
    df_15m["vwap_15m"] = (
        (df_15m["close"] * df_15m["volume"]).rolling(20, min_periods=5).sum()
        / df_15m["volume"].rolling(20, min_periods=5).sum()
    )
    slope = df_15m["cvd"].diff(19)
    umbral = df_15m["volume"].rolling(20).mean() * 0.5
    df_15m["cvd_trend"] = np.select([slope > umbral, slope < -umbral], ["UP", "DOWN"], default="FLAT")
    df_15m["atr14_15m"] = _atr(df_15m, 14)

    # Percentil rolling del ATR (normalizado por precio, no en dolares --
    # BTC a 70k vs 60k no debe leerse como "mas volatil") contra su propia
    # historia de 30 dias -- ver atr_compression_percentile en HTFParams.
    # min_periods=ATR_RANK_LOOKBACK_BARS//4 (~7.5 dias): antes de eso el
    # percentil seria contra muy poca historia, mejor NaN (decide() lo trata
    # como "sin filtro todavia") que un percentil ruidoso.
    atr_pct = df_15m["atr14_15m"] / df_15m["close"]
    df_15m["atr_rank_30d"] = atr_pct.rolling(ATR_RANK_LOOKBACK_BARS, min_periods=ATR_RANK_LOOKBACK_BARS // 4).rank(pct=True)

    # merge_asof backward: en cada vela 15m, el POC/VAH/VAL/swing visibles
    # son los de la última vela 1h/4h YA CERRADA antes de esa marca de
    # tiempo - nunca la que está en curso ni ninguna futura.
    df_15m = pd.merge_asof(
        df_15m.sort_values("open_time"),
        df_1h[["open_time", "poc", "vah", "val"]].sort_values("open_time"),
        on="open_time", direction="backward",
    )
    df_15m = pd.merge_asof(
        df_15m,
        df_4h[["open_time", "swing_h4_h", "swing_h4_l"]].sort_values("open_time"),
        on="open_time", direction="backward",
    )
    return df_15m


# ----------------------------------------------------------------------
# Precomputo M1 (vectorizado)
# ----------------------------------------------------------------------

def calcular_features_1m(df_1m: pd.DataFrame, params: HTFParams) -> pd.DataFrame:
    print("🧮 Precalculando features 1m (rolling vectorizado)...")
    df = df_1m.copy()

    df["high_20"] = df["high"].rolling(20).max().shift(1)
    df["low_20"] = df["low"].rolling(20).min().shift(1)
    df["avg_abs_delta_10"] = df["delta"].abs().rolling(10).mean().shift(1)
    df["range_high_10"] = df["high"].rolling(10).max().shift(1)
    df["range_low_10"] = df["low"].rolling(10).min().shift(1)
    df["vol_prom_10"] = df["volume"].rolling(10).mean().shift(1)
    df["vol_p95_2h"] = df["volume"].rolling(120).quantile(0.95).shift(1)

    rango = df["high"] - df["low"]
    flip_ratio = df["delta"].abs() / df["volume"].replace(0, np.nan)
    df["abs"] = (
        (rango > 0) & (df["volume"] > 0) & (df["volume"] >= df["vol_p95_2h"])
        & (flip_ratio > params.absorption_flip_ratio) & (rango < df["close"] * 0.001)
    ).fillna(False)

    # default="" (no None): pandas convierte None a NaN al asignarlo a una
    # columna, y bool(float('nan')) es True en Python - con default=None,
    # brain_htf_funding.prioridad_setup() interpretaba TODAS las velas como
    # "hay stop_run" (falso positivo permanente). "" sí es genuinamente
    # falsy y sobrevive la asignación a la columna sin coerción.
    cond_short = (df["high"] > df["high_20"]) & (df["close"] <= df["high_20"]) & (df["delta"] < 0)
    cond_long = (df["low"] < df["low_20"]) & (df["close"] >= df["low_20"]) & (df["delta"] > 0)
    df["stop_run"] = np.select([cond_short, cond_long], ["SHORT", "LONG"], default="")

    rango_10_pct = (df["range_high_10"] - df["range_low_10"]) / df["close"]
    vol_ok = df["volume"] >= params.breakout_vol_mult * df["vol_prom_10"]
    cond_up = (rango_10_pct <= params.breakout_range_pct) & vol_ok & (df["delta"] > 0)
    cond_down = (rango_10_pct <= params.breakout_range_pct) & vol_ok & (df["delta"] < 0)
    df["breakout_vol"] = np.select([cond_up, cond_down], ["LONG", "SHORT"], default="")

    print("🧮 Marcando divergencia de delta (orderflow_data._mark_delta_divergence)...")
    velas = df[["close", "delta"]].rename(columns={"close": "c"}).to_dict("records")
    for v in velas:
        v["div"] = False  # _mark_delta_divergence solo escribe True, nunca inicializa la clave
    ofd._mark_delta_divergence(velas)
    df["div"] = [v["div"] for v in velas]

    return df


def agregar_initiative_pullback(df_1m: pd.DataFrame, params: HTFParams) -> pd.DataFrame:
    df = df_1m.copy()
    precio = df["close"]
    dist_poc = (precio - df["poc"]).abs() / precio
    dist_vah = (precio - df["vah"]).abs() / precio
    dist_val = (precio - df["val"]).abs() / precio
    en_zona_valor = (dist_poc < params.pullback_htf_distance_pct) | (dist_vah < params.pullback_htf_distance_pct) | (dist_val < params.pullback_htf_distance_pct)

    tendencia_up = df["cvd_trend"] == "UP"
    tendencia_down = df["cvd_trend"] == "DOWN"
    exhausted = df["avg_abs_delta_10"].notna() & (df["avg_abs_delta_10"] > 0) & (df["delta"].abs() <= params.pullback_delta_exhaustion_pct * df["avg_abs_delta_10"])

    cond_long = en_zona_valor & tendencia_up & (df["delta"] < 0) & exhausted
    cond_short = en_zona_valor & tendencia_down & (df["delta"] > 0) & exhausted
    df["initiative_pullback"] = np.select([cond_long, cond_short], ["LONG", "SHORT"], default="")
    return df


# ----------------------------------------------------------------------
# Simulación de una ventana (test_start, test_end)
# ----------------------------------------------------------------------

def simular_ventana(df: pd.DataFrame, params: HTFParams, asset_cfg: dict) -> list:
    """Recorre df (ya recortado a la ventana de test) barra a barra,
    llama brain.decide() y gestiona la posición abierta hasta cerrarla
    (TP1 parcial -> breakeven -> TP2 -> MAX_HOLD). Una posición a la vez.

    El brain ya devuelve niveles NETOS de costos (calc_net_levels, sobre el
    precio de la vela que generó la señal) para decidir si la señal vale la
    pena - pero el fill real ocurre en el open de la SIGUIENTE barra, a un
    precio distinto. Para no contar el spread/slippage dos veces, acá se
    reaplican las MISMAS distancias relativas (sl/tp1/tp2 - entry) que
    decidió el brain, ancladas al precio de mercado real de fill; el spread
    y el slippage de esa entrada real se aplican una sola vez, ahí."""
    brain = HTFFundingBrain(params, asset_cfg)
    spread_slip = (asset_cfg["spread_bps"] + asset_cfg["slippage_bps"]) / 10000
    trades = []
    posicion = None  # dict con el estado de la posición abierta, o None

    filas = df.to_dict("records")
    for i, fila in enumerate(filas):
        ts_ms = int(fila["open_time"].timestamp() * 1000)

        if posicion is not None:
            posicion = _gestionar_posicion(posicion, fila, params, asset_cfg)
            if posicion is not None and posicion.get("cerrada"):
                trades.append(posicion)
                posicion = None
            continue  # una posición a la vez - no evaluar señales nuevas mientras hay una abierta

        htf = {
            "poc": fila.get("poc"), "vah": fila.get("vah"), "val": fila.get("val"),
            "swing_h4_h": fila.get("swing_h4_h"), "swing_h4_l": fila.get("swing_h4_l"),
            "vwap_15m": fila.get("vwap_15m"), "cvd_trend": fila.get("cvd_trend"),
            "atr14_15m": fila.get("atr14_15m"),
            "atr_rank_30d": fila.get("atr_rank_30d") if pd.notna(fila.get("atr_rank_30d")) else None,
        }
        if any(pd.isna(v) for v in (htf["poc"], htf["atr14_15m"])):
            continue  # HTF sin warmup suficiente todavia

        senal = brain.decide(fila, htf, ts_ms)
        if senal is None:
            continue

        # Fill en el OPEN de la siguiente barra (no en el close de la
        # señal - evita look-ahead de ejecutar al precio que generó la
        # señal), con spread+slippage reales del asset.
        if i + 1 >= len(filas):
            continue
        fila_fill = filas[i + 1]
        direccion = senal["decision"]
        entry_real = fila_fill["open"] * (1 + spread_slip) if direccion == "LONG" else fila_fill["open"] * (1 - spread_slip)

        sl_dist = abs(senal["entry_price"] - senal["stop_loss_price"])
        tp1_dist = abs(senal["tp1_price"] - senal["entry_price"])
        tp2_dist = abs(senal["tp2_price"] - senal["entry_price"])
        signo = 1 if direccion == "LONG" else -1
        sl_real = entry_real - signo * sl_dist
        tp1_real = entry_real + signo * tp1_dist
        tp2_real = entry_real + signo * tp2_dist

        swap_bps_8h = asset_cfg["swap_long_bps_8h"] if direccion == "LONG" else asset_cfg["swap_short_bps_8h"]

        posicion = {
            "direccion": direccion, "setup_type": senal["setup_type"], "conviction": senal["conviction"],
            "entry_price": entry_real, "sl_price": sl_real, "tp1_price": tp1_real, "tp2_price": tp2_real,
            "tp1_size_pct": senal["tp1_size_pct"], "atr": senal["atr14_15m"], "swap_bps_8h": swap_bps_8h,
            "riesgo_original": sl_dist, "cost_bps": senal["cost_bps"], "rr_net": senal["rr_net"],
            "qty_abierta": 1.0, "qty_tp1": senal["tp1_size_pct"], "tp1_hit": False, "pnl_r": 0.0,
            "entry_ts": fila_fill["open_time"], "cerrada": False, "params": params,
            "commission_mode": asset_cfg["commission_mode"],
            "fee_bps": asset_cfg["fee_bps"],
            "commission_fixed_usdt_per_side": asset_cfg["commission_fixed_usdt_per_side"],
        }

    return [t for t in trades if t.get("motivo_cierre")]


def _gestionar_posicion(pos: dict, fila: dict, params: HTFParams, asset_cfg: dict) -> dict:
    direccion = pos["direccion"]
    high, low = fila["high"], fila["low"]
    hold_horas = (fila["open_time"] - pos["entry_ts"]).total_seconds() / 3600

    def _cerrar(precio_salida: float, motivo: str, qty: float, cierre_final: bool) -> None:
        r_unitario = abs(precio_salida - pos["entry_price"]) / abs(pos["entry_price"] - pos["sl_price"])
        signo = 1 if (precio_salida - pos["entry_price"]) * (1 if direccion == "LONG" else -1) > 0 else -1
        pos["pnl_r"] += signo * r_unitario * qty
        if cierre_final:
            # Swap real, proporcional a las horas de hold reales (no la
            # estimación que usó el brain para decidir) - se resta siempre,
            # nunca suma, salvo que swap_bps_8h sea positivo (short en un
            # activo con funding negativo para el long, ver config/assets.py).
            swap_cost_precio = pos["entry_price"] * (pos["swap_bps_8h"] / 10000) * (hold_horas / 8.0)
            pos["pnl_r"] += swap_cost_precio / pos["riesgo_original"]

            # Comision ida+vuelta (nunca se aplicaba antes de este cambio -
            # ver nota en BROKER_COST_OVERLAYS). Siempre resta, nunca suma.
            if pos["commission_mode"] == "FIXED_USD":
                commission_cost_precio = pos["commission_fixed_usdt_per_side"] * 2
            else:  # PCT_NOTIONAL
                commission_cost_precio = pos["entry_price"] * (pos["fee_bps"] / 10000) * 2
            pos["pnl_r"] -= commission_cost_precio / pos["riesgo_original"]
        pos.setdefault("motivo_cierre", motivo)

    if not pos["tp1_hit"]:
        if direccion == "LONG":
            sl_hit, tp1_hit = low <= pos["sl_price"], high >= pos["tp1_price"]
        else:
            sl_hit, tp1_hit = high >= pos["sl_price"], low <= pos["tp1_price"]

        if sl_hit:
            _cerrar(pos["sl_price"], "SL", 1.0, cierre_final=True)
            pos["cerrada"] = True
            pos["exit_ts"] = fila["open_time"]
            return pos
        if tp1_hit:
            _cerrar(pos["tp1_price"], "TP1_PARCIAL", pos["qty_tp1"], cierre_final=False)
            pos["tp1_hit"] = True
            pos["qty_abierta"] = 1.0 - pos["qty_tp1"]
            be_buffer = pos["atr"] * params.be_buffer_atr_mult
            pos["sl_price"] = pos["entry_price"] + be_buffer if direccion == "LONG" else pos["entry_price"] - be_buffer
            pos.pop("motivo_cierre", None)  # el TP1 parcial no cierra el trade, sigue con el runner
    else:
        if direccion == "LONG":
            sl_hit, tp2_hit = low <= pos["sl_price"], high >= pos["tp2_price"]
        else:
            sl_hit, tp2_hit = high >= pos["sl_price"], low <= pos["tp2_price"]

        if sl_hit:
            _cerrar(pos["sl_price"], "BE_STOP", pos["qty_abierta"], cierre_final=True)
            pos["cerrada"] = True
            pos["exit_ts"] = fila["open_time"]
            return pos
        if tp2_hit:
            _cerrar(pos["tp2_price"], "TP2", pos["qty_abierta"], cierre_final=True)
            pos["cerrada"] = True
            pos["exit_ts"] = fila["open_time"]
            return pos

    if hold_horas >= params.max_hold_hours:
        qty_restante = pos["qty_abierta"] if pos["tp1_hit"] else 1.0
        _cerrar(fila["close"], "MAX_HOLD", qty_restante, cierre_final=True)
        pos["cerrada"] = True
        pos["exit_ts"] = fila["open_time"]

    return pos


# ----------------------------------------------------------------------
# Métricas
# ----------------------------------------------------------------------

def calcular_metricas(trades: list, dias_ventana: float, capital_base: float = 50_000.0, risk_pct_trade: float = 0.005) -> dict:
    """capital_base/risk_pct_trade: para expresar max_dd_pct hace falta una
    base monetaria (0.5% de riesgo nominal por trade, igual que
    RISK_PER_TRADE_BASE del cerebro principal, sobre $50,000) - medir el
    drawdown directo sobre la curva de R-múltiplos es matemáticamente
    inestable (el "pico" acumulado puede pasar cerca de 0 al principio de
    la serie, produciendo porcentajes sin sentido como >1000%)."""
    if not trades:
        return {
            "total_trades": 0, "win_rate": 0.0, "profit_factor": 0.0, "avg_r": 0.0,
            "max_dd_pct": 0.0, "avg_hold_hours": 0.0, "trades_per_month": 0.0,
            "avg_conviction": 0.0, "cost_bps_avg": 0.0, "rr_net_avg": 0.0, "by_direction": {},
        }

    r_multiples = [t["pnl_r"] for t in trades]
    wins = [r for r in r_multiples if r > 0]
    losses = [r for r in r_multiples if r <= 0]
    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))

    pnl_usdt = np.array(r_multiples) * risk_pct_trade * capital_base
    equity_curve = capital_base + np.cumsum(pnl_usdt)
    peak = np.maximum.accumulate(equity_curve)
    dd = (peak - equity_curve) / peak
    max_dd_pct = float(np.max(dd)) if len(dd) else 0.0

    hold_horas = [
        (t["exit_ts"] - t["entry_ts"]).total_seconds() / 3600
        for t in trades if t.get("exit_ts") is not None
    ]

    meses = max(dias_ventana / 30.44, 1e-9)

    por_direccion = {}
    for d in ("LONG", "SHORT"):
        sub = [t["pnl_r"] for t in trades if t["direccion"] == d]
        if sub:
            sub_wins = [r for r in sub if r > 0]
            por_direccion[d] = {
                "total_trades": len(sub), "win_rate": round(len(sub_wins) / len(sub), 4),
                "avg_r": round(float(np.mean(sub)), 4),
            }

    return {
        "total_trades": len(trades),
        "win_rate": round(len(wins) / len(trades), 4),
        "profit_factor": round(gross_profit / gross_loss, 4) if gross_loss > 0 else float("inf"),
        "avg_r": round(float(np.mean(r_multiples)), 4),
        "max_dd_pct": round(max_dd_pct, 4),
        "avg_hold_hours": round(float(np.mean(hold_horas)), 2) if hold_horas else 0.0,
        "trades_per_month": round(len(trades) / meses, 2),
        "avg_conviction": round(float(np.mean([t["conviction"] for t in trades])), 2),
        "cost_bps_avg": round(float(np.mean([t["cost_bps"] for t in trades])), 2),
        "rr_net_avg": round(float(np.mean([t["rr_net"] for t in trades])), 3),
        "by_direction": por_direccion,
    }


def _evaluar_pass(metricas_agregadas: dict) -> dict:
    resultado = {}
    for metrica in ("win_rate", "profit_factor", "max_dd_pct"):
        target = PASS_TARGETS[metrica]
        actual = metricas_agregadas.get(metrica, 0.0)
        ok = actual <= target if metrica == "max_dd_pct" else actual >= target
        resultado[metrica] = {"target": target, "actual": actual, "pass": bool(ok)}

    actual_tpm = metricas_agregadas.get("trades_per_month", 0.0)
    tmin, tmax = PASS_TARGETS["trades_per_month_min"], PASS_TARGETS["trades_per_month_max"]
    resultado["trades_per_month"] = {
        "target": f"{tmin:.0f}-{tmax:.0f}", "actual": actual_tpm, "pass": bool(tmin <= actual_tpm <= tmax),
    }
    return resultado


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Backtest walk-forward del cerebro HTF de fondeo")
    parser.add_argument("--wf", type=int, default=3, help="Cantidad de ventanas walk-forward")
    parser.add_argument("--symbol", default="BTCUSDT", choices=["BTCUSDT"],
                         help="Único símbolo con datos reales completos en este repo por ahora")
    parser.add_argument("--cost-model", type=str, default="BINANCE_FUTURES_CURRENT", choices=list(BROKER_COST_OVERLAYS.keys()), help="overlay de costos de broker/cuenta (ver BROKER_COST_OVERLAYS)")
    args = parser.parse_args()
    asset_cfg = _apply_cost_overlay(get_asset_config(args.symbol), args.cost_model)

    t0 = time.perf_counter()
    print(f"📊 Cargando 11 CSV mensuales reales ({args.symbol} spot 1m, ene-nov 2025)...")
    df_1m_raw = cargar_1m()
    print(f"   {len(df_1m_raw)} velas 1m | {df_1m_raw['open_time'].min()} -> {df_1m_raw['open_time'].max()}")

    df_15m_raw = ofd.resample_klines(df_1m_raw, "15min")
    df_1h_raw = ofd.resample_klines(df_1m_raw, "1h")
    print(f"   15m: {len(df_15m_raw)} velas | 1h: {len(df_1h_raw)} velas")

    params = HTFParams()
    df_15m = calcular_htf(df_1h_raw, df_15m_raw)

    df_1m = calcular_features_1m(df_1m_raw, params)
    df_1m = pd.merge_asof(
        df_1m.sort_values("open_time"),
        df_15m[["open_time", "poc", "vah", "val", "swing_h4_h", "swing_h4_l", "vwap_15m", "cvd_trend", "atr14_15m", "atr_rank_30d"]].sort_values("open_time"),
        on="open_time", direction="backward",
    )
    df_1m = agregar_initiative_pullback(df_1m, params)
    print(f"⏱️ Precómputo total: {time.perf_counter() - t0:.1f}s")

    fecha_min, fecha_max = df_1m["open_time"].min(), df_1m["open_time"].max()
    dias_totales = (fecha_max - fecha_min).days
    warmup_dias = 60  # margen para que POC/VAH/VAL/ATR/swing H4 tengan historia real antes del primer test

    n_ventanas = args.wf
    dias_test = max((dias_totales - warmup_dias) // n_ventanas, 1)

    ventanas = []
    for w in range(n_ventanas):
        test_start = fecha_min + pd.Timedelta(days=warmup_dias + w * dias_test)
        test_end = fecha_min + pd.Timedelta(days=warmup_dias + (w + 1) * dias_test)
        if w == n_ventanas - 1:
            test_end = fecha_max
        ventanas.append((test_start, test_end))

    resultados_ventanas = []
    todos_los_trades = []
    for idx, (test_start, test_end) in enumerate(ventanas, 1):
        print(f"\n🧪 Ventana {idx}: test {test_start.date()} -> {test_end.date()}")
        df_ventana = df_1m[(df_1m["open_time"] >= test_start) & (df_1m["open_time"] < test_end)].reset_index(drop=True)
        trades = simular_ventana(df_ventana, params, asset_cfg)
        dias_ventana = (test_end - test_start).total_seconds() / 86400
        metricas = calcular_metricas(trades, dias_ventana)
        print(f"   Trades: {metricas['total_trades']} | WR: {metricas['win_rate']:.1%} | PF: {metricas['profit_factor']:.2f} | DD: {metricas['max_dd_pct']:.1%}")
        resultados_ventanas.append({
            "test": f"{test_start.date()}..{test_end.date()}", "metrics": metricas,
        })
        todos_los_trades.extend(trades)

    dias_agregado = (ventanas[-1][1] - ventanas[0][0]).total_seconds() / 86400
    agregadas = calcular_metricas(todos_los_trades, dias_agregado)
    pass_criteria = _evaluar_pass(agregadas)

    output = {
        "symbol": args.symbol,
        "asset_class": asset_cfg["asset_class"],
        "config_used": asset_cfg,
        "params_brain": {
            "sl_atr_mult": params.sl_atr_mult, "tp1_atr_mult": params.tp1_atr_mult, "tp2_atr_mult": params.tp2_atr_mult,
            "tp1_size_pct": params.tp1_size_pct, "min_conviction": params.min_conviction,
            "zone_proximity_pct": params.zone_proximity_pct, "max_hold_hours": params.max_hold_hours,
            "counter_trend_conviction": params.counter_trend_conviction,
            "be_trigger_atr_mult": params.be_trigger_atr_mult, "be_buffer_atr_mult": params.be_buffer_atr_mult,
            "min_tp1_spread_mult": params.min_tp1_spread_mult, "min_rr_net": params.min_rr_net,
            "fuente_datos": f"{args.symbol} spot 1m real (binance-public-data), ene-nov 2025, 15m/1h resampleados del mismo 1m",
        },
        "windows": resultados_ventanas,
        "aggregated": agregadas,
        "pass_criteria": pass_criteria,
    }

    RESULTS_DIR.mkdir(exist_ok=True)
    cost_part = f"_{args.cost_model.lower()}" if args.cost_model != "BINANCE_FUTURES_CURRENT" else ""
    out_path = RESULTS_DIR / f"{args.symbol.lower()}_htf_wf{cost_part}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    out_path.write_text(json.dumps(output, indent=2, ensure_ascii=False, default=str), encoding="utf-8")

    print("\n" + "=" * 60)
    print("=== WALK-FORWARD RESULT ===")
    for i, w in enumerate(resultados_ventanas, 1):
        m = w["metrics"]
        print(f"Window {i} (test {w['test']}): WR={m['win_rate']:.1%} PF={m['profit_factor']:.2f} DD={m['max_dd_pct']:.1%} Trades={m['total_trades']}")
    todas_pass = all(p["pass"] for p in pass_criteria.values())
    print(
        f"AGGREGATED: WR={agregadas['win_rate']:.1%} PF={agregadas['profit_factor']:.2f} "
        f"DD={agregadas['max_dd_pct']:.1%} Trades/mo={agregadas['trades_per_month']:.1f} "
        f"{'✅ ALL PASS' if todas_pass else '❌ NO PASS'}"
    )
    for metrica, r in pass_criteria.items():
        estado = "✅" if r["pass"] else "❌"
        print(f"  {estado} {metrica}: target={r['target']} actual={r['actual']}")
    print(f"\n💾 Resultado guardado en {out_path}")
    print("=" * 60)


if __name__ == "__main__":
    main()
