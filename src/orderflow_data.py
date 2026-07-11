"""
Carga y feature engineering de Order Flow a partir de klines de Binance
(BTCUSDT_1h/5m/15m/M1_completo.csv en DATA CSV/BTCUSD).

IMPORTANTE - qué es real y qué es aproximado:
Estos CSV tienen el formato estándar de klines de Binance (12 columnas: open_time,
OHLCV, close_time, quote_volume, num_trades, taker_buy_base_vol, taker_buy_quote_vol,
ignore). Son datos AGREGADOS por vela, no tick-by-tick ni profundidad L2 del book.

- Delta y CVD: REALES. Se derivan de taker_buy_base_vol (volumen comprador agresivo)
  vs el volumen vendedor agresivo implícito (volume - taker_buy_base_vol).
- Volume Profile (POC/VAH/VAL) y Naked POCs: APROXIMADOS. Se distribuye el volumen
  de cada vela uniformemente entre su High y Low (no tenemos el precio exacto de
  cada trade).
- Liquidity clusters (equal highs/lows): APROXIMADOS, vía detección de pivotes.
- tape_speed: REAL (num_trades / duración de la vela).
- Imbalances 3:1 por nivel de precio, iceberg refresh y bookmap L2: NO CALCULABLES
  con este dataset (requieren tick data u order book L2). Se devuelven vacíos/0 en
  vez de inventarse - ver `build_ltf_candles` y `empty_bookmap`.
"""
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

KLINE_COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_volume", "num_trades",
    "taker_buy_base_vol", "taker_buy_quote_vol", "ignore",
]

DATA_DIR = Path(
    "C:/Users/ezequiel/OneDrive/Desktop/DATA CSV/BTCUSD/"
    "BTCUSD dataset de Order Flow (Flujo de Órdenes) completo de nivel institucional"
)

FILES = {
    "1h": "BTCUSDT_1h_completo.csv",
    "15m": "BTCUSDT_15m_completo.csv",
    "5m": "BTCUSDT_5m_completo.csv",
    "1m": "BTCUSDT_M1_1year_completo.csv",
}


def load_klines(path: Path, tail: Optional[int] = None) -> pd.DataFrame:
    """Lee un CSV de klines. Usa solo las primeras 12 columnas: algunos archivos
    (p.ej. el M1 de 1 año) tienen columnas vacías extra en la fila 1 por un bug
    de exportación, pero los datos reales siempre están en las primeras 12."""
    df = pd.read_csv(path, header=None, usecols=range(12), names=KLINE_COLUMNS, low_memory=False)

    # Algunas filas (p.ej. la fila 1 del M1 de 1 año) traen valores corruptos tipo
    # "107146.51000000.1" (doble punto decimal) por un bug de exportación previo.
    # Se fuerza a numérico y se descartan las filas que no puedan convertirse.
    numeric_cols = ["open", "high", "low", "close", "volume", "quote_volume",
                     "num_trades", "taker_buy_base_vol", "taker_buy_quote_vol"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["open_time"] = pd.to_datetime(df["open_time"], unit="us", errors="coerce")
    df["close_time"] = pd.to_datetime(df["close_time"], unit="us", errors="coerce")
    df = df.dropna(subset=["open_time", "close_time", *numeric_cols])
    df = df.sort_values("open_time").drop_duplicates("open_time").reset_index(drop=True)

    taker_sell_vol = df["volume"] - df["taker_buy_base_vol"]
    df["delta"] = df["taker_buy_base_vol"] - taker_sell_vol
    df["cvd"] = df["delta"].cumsum()

    if tail:
        df = df.tail(tail).reset_index(drop=True)
    return df


def resample_klines(df: pd.DataFrame, rule: str) -> pd.DataFrame:
    """Reagrupa velas a un timeframe mayor (p.ej. 1m -> 3m), recalculando
    delta/cvd sobre los volúmenes agregados (no sobre el delta promedio)."""
    if df.empty:
        return df

    base = df.set_index("open_time")
    agg = base.resample(rule).agg({
        "open": "first", "high": "max", "low": "min", "close": "last",
        "volume": "sum", "close_time": "last", "quote_volume": "sum",
        "num_trades": "sum", "taker_buy_base_vol": "sum", "taker_buy_quote_vol": "sum",
    }).dropna(subset=["open"])

    agg = agg.reset_index()
    taker_sell_vol = agg["volume"] - agg["taker_buy_base_vol"]
    agg["delta"] = agg["taker_buy_base_vol"] - taker_sell_vol
    agg["cvd"] = agg["delta"].cumsum()
    return agg


def load_symbol(symbol_dir: Path = DATA_DIR, files: dict = FILES) -> dict:
    """Carga los 4 timeframes disponibles. Devuelve dict {tf: DataFrame}."""
    resultado = {}
    for tf, filename in files.items():
        path = symbol_dir / filename
        if not path.exists():
            continue
        resultado[tf] = load_klines(path)
    return resultado


# ----------------------------------------------------------------------
# Volume Profile (POC / VAH / VAL) - aproximado
# ----------------------------------------------------------------------

def volume_profile(df: pd.DataFrame, bins: int = 50, value_area_pct: float = 0.70) -> dict:
    if df.empty:
        return {"poc": 0.0, "vah": 0.0, "val": 0.0}

    lo, hi = float(df["low"].min()), float(df["high"].max())
    if hi <= lo:
        precio = float(df["close"].iloc[-1])
        return {"poc": precio, "vah": precio, "val": precio}

    edges = np.linspace(lo, hi, bins + 1)
    vol_by_bin = np.zeros(bins)

    lows = df["low"].to_numpy()
    highs = df["high"].to_numpy()
    vols = df["volume"].to_numpy()

    for row_lo, row_hi, vol in zip(lows, highs, vols):
        if row_hi <= row_lo or vol <= 0:
            continue
        i0 = max(0, min(int(np.searchsorted(edges, row_lo, side="right")) - 1, bins - 1))
        i1 = max(0, min(int(np.searchsorted(edges, row_hi, side="right")) - 1, bins - 1))
        span = i1 - i0 + 1
        vol_by_bin[i0:i1 + 1] += vol / span

    poc_idx = int(np.argmax(vol_by_bin))
    poc = float((edges[poc_idx] + edges[poc_idx + 1]) / 2)

    order = np.argsort(vol_by_bin)[::-1]
    total_vol = vol_by_bin.sum()
    target = total_vol * value_area_pct
    acc = 0.0
    included = []
    for idx in order:
        if vol_by_bin[idx] <= 0:
            break
        acc += vol_by_bin[idx]
        included.append(int(idx))
        if acc >= target:
            break

    vah = float(edges[max(included) + 1]) if included else poc
    val = float(edges[min(included)]) if included else poc

    return {"poc": round(poc, 2), "vah": round(vah, 2), "val": round(val, 2)}


def naked_pocs(df_htf: pd.DataFrame, max_results: int = 5) -> list:
    """POCs de días previos que el precio no volvió a tocar (por eso 'naked').
    Se calcula sobre el timeframe HTF (1h) agrupando por día calendario."""
    if df_htf.empty:
        return []

    daily = df_htf.copy()
    daily["date"] = daily["open_time"].dt.date
    dates = sorted(daily["date"].unique())

    resultado = []
    for d in dates[:-1]:
        day_df = daily[daily["date"] == d]
        if len(day_df) < 3:
            continue
        vp = volume_profile(day_df, bins=20)
        poc = vp["poc"]

        later = daily[daily["date"] > d]
        tocado = bool(((later["low"] <= poc) & (later["high"] >= poc)).any())
        if not tocado:
            resultado.append(poc)

    return resultado[-max_results:]


# ----------------------------------------------------------------------
# Liquidez (equal highs/lows via pivotes) - aproximado
# ----------------------------------------------------------------------

def _pivots(high: np.ndarray, low: np.ndarray, izq: int = 2, der: int = 2):
    n = len(high)
    ph = np.zeros(n, dtype=bool)
    pl = np.zeros(n, dtype=bool)
    for i in range(izq, n - der):
        if high[i] == high[i - izq:i + der + 1].max():
            ph[i] = True
        if low[i] == low[i - izq:i + der + 1].min():
            pl[i] = True
    return ph, pl


def liquidity_clusters(df_htf: pd.DataFrame, lookback: int = 150, tol_pct: float = 0.0008) -> dict:
    """Agrupa swing highs/lows cercanos entre sí: son niveles donde probablemente
    hay stops/liquidez pasiva acumulada. 'size' es la cantidad de toques (proxy,
    no el tamaño real del book que no tenemos)."""
    recent = df_htf.tail(lookback)
    if len(recent) < 10:
        return {"bids": [], "asks": []}

    high = recent["high"].to_numpy()
    low = recent["low"].to_numpy()
    ph, pl = _pivots(high, low)

    tol = float(recent["close"].mean()) * tol_pct

    def agrupar(vals):
        vals = sorted(vals)
        grupos = []
        for v in vals:
            if grupos and abs(v - grupos[-1][-1]) <= tol:
                grupos[-1].append(v)
            else:
                grupos.append([v])
        return [(round(sum(g) / len(g), 2), len(g)) for g in grupos if len(g) > 1]

    asks = agrupar(high[ph].tolist())
    bids = agrupar(low[pl].tolist())
    return {"bids": bids, "asks": asks}


def cvd_trend(df: pd.DataFrame, lookback: int = 20) -> str:
    if len(df) < 2:
        return "FLAT"
    recent = df["cvd"].tail(lookback)
    slope = recent.iloc[-1] - recent.iloc[0]
    umbral = df["volume"].tail(lookback).mean() * 0.5
    if slope > umbral:
        return "UP"
    if slope < -umbral:
        return "DOWN"
    return "FLAT"


# ----------------------------------------------------------------------
# LTF candles para el snapshot (1m / 3m)
# ----------------------------------------------------------------------

def build_ltf_candles(df: pd.DataFrame, n: int = 30) -> list:
    """Arma la lista de velas LTF para el snapshot. `imb` e `iceberg` quedan
    vacíos/0 porque no son calculables sin datos tick-by-tick / L2 (ver docstring
    del módulo) - no se inventan valores."""
    recent = df.tail(n).copy()
    if recent.empty:
        return []

    avg_abs_delta = recent["delta"].abs().rolling(20, min_periods=5).mean()
    rango = recent["high"] - recent["low"]
    rango_medio = rango.rolling(20, min_periods=5).mean()

    velas = []
    for i, row in enumerate(recent.itertuples()):
        delta_avg = avg_abs_delta.iloc[i] if not pd.isna(avg_abs_delta.iloc[i]) else abs(row.delta)
        rango_avg = rango_medio.iloc[i] if not pd.isna(rango_medio.iloc[i]) else (row.high - row.low)

        # Absorción (aproximada): delta extremo vs su propio promedio reciente,
        # pero rango de la vela chico (el precio "no avanzó" pese al volumen agresivo)
        es_absorcion = bool(
            delta_avg > 0
            and abs(row.delta) > 1.5 * delta_avg
            and rango_avg > 0
            and (row.high - row.low) < 0.7 * rango_avg
        )

        velas.append({
            "t": row.open_time.strftime("%H:%M:%S"),
            "c": round(float(row.close), 2),
            "delta": round(float(row.delta), 4),
            "vol": round(float(row.volume), 4),
            "imb": [],       # no calculable con klines agregados (requiere tick/L2)
            "abs": es_absorcion,
            "iceberg": 0,    # no calculable con klines agregados (requiere L2)
            "div": False,    # se completa a nivel de snapshot (compara con swings previos)
        })

    _mark_delta_divergence(velas)
    return velas


def _mark_delta_divergence(velas: list) -> None:
    """Marca divergencia si el precio hace nuevo extremo pero con delta menor
    al del extremo anterior en la misma dirección."""
    if len(velas) < 6:
        return
    closes = [v["c"] for v in velas]
    for i in range(5, len(velas)):
        ventana = closes[max(0, i - 5):i]
        if velas[i]["c"] >= max(ventana) and velas[i]["delta"] > 0:
            prev_deltas = [v["delta"] for v in velas[max(0, i - 5):i] if v["delta"] > 0]
            if prev_deltas and velas[i]["delta"] < max(prev_deltas):
                velas[i]["div"] = True
        elif velas[i]["c"] <= min(ventana) and velas[i]["delta"] < 0:
            prev_deltas = [v["delta"] for v in velas[max(0, i - 5):i] if v["delta"] < 0]
            if prev_deltas and velas[i]["delta"] > min(prev_deltas):
                velas[i]["div"] = True


def tape_speed(df: pd.DataFrame, n: int = 5) -> int:
    """Trades por minuto, promedio de las últimas n velas (num_trades es real)."""
    recent = df.tail(n)
    if recent.empty:
        return 0
    duracion_min = (recent["close_time"] - recent["open_time"]).dt.total_seconds().mean() / 60
    if duracion_min <= 0:
        return 0
    return int(recent["num_trades"].mean() / duracion_min)


def empty_bookmap() -> dict:
    """No hay datos L2/order book en este dataset (solo klines agregados).
    Se devuelve la estructura vacía en vez de inventar paredes/iceberg falsos."""
    return {"bid_iceberg": 0, "ask_walls": []}




def zone_reaction_history(
    df: pd.DataFrame,
    nivel: float,
    direccion: str,
    tolerancia_pct: float = 0.0015,
    ventana_velas: int = 20,
    min_move_pct: float = 0.3,
    excluir_ultimas: int = 3,
) -> Optional[dict]:
    """Backtest real: busca en `df` (histórico, p.ej. 15m) toques previos de `nivel`
    y mide cuánto se movió el precio a favor en las siguientes `ventana_velas`.
    Todo lo que devuelve sale de datos reales, no son estimaciones de Gemini.

    direccion: "LONG" (mide rebote hacia arriba) o "SHORT" (rebote hacia abajo).
    excluir_ultimas: no cuenta las últimas N velas como "toque previo" (evita que
    el propio toque actual que genera la señal se cuente a sí mismo).
    """
    if df.empty or len(df) < 30:
        return None

    tol = nivel * tolerancia_pct
    historico = df.iloc[:-excluir_ultimas] if excluir_ultimas else df

    if direccion == "LONG":
        toques_mask = (historico["low"] >= nivel - tol) & (historico["low"] <= nivel + tol)
    else:
        toques_mask = (historico["high"] >= nivel - tol) & (historico["high"] <= nivel + tol)

    indices = historico.index[toques_mask].tolist()
    if not indices:
        return None

    # Agrupar toques consecutivos en un solo "evento" (mismo testeo de zona)
    eventos = []
    for idx in indices:
        if not eventos or idx - eventos[-1] > 3:
            eventos.append(idx)

    reacciones = []
    for idx in eventos:
        fin = min(idx + ventana_velas, len(df) - 1)
        ventana = df.iloc[idx:fin + 1]
        if direccion == "LONG":
            pct = float((ventana["high"].max() - nivel) / nivel * 100)
        else:
            pct = float((nivel - ventana["low"].min()) / nivel * 100)
        reacciones.append({"pct": pct, "idx": idx})

    if not reacciones:
        return None

    ganadoras = [r for r in reacciones if r["pct"] >= min_move_pct]
    win_rate = round(len(ganadoras) / len(reacciones) * 100)

    tp_prom_pct = round(sum(r["pct"] for r in reacciones) / len(reacciones), 2)
    tp_max_pct = round(max(r["pct"] for r in reacciones), 2)
    tp_prom_price = round(nivel * (1 + tp_prom_pct / 100), 2) if direccion == "LONG" else round(nivel * (1 - tp_prom_pct / 100), 2)
    tp_max_price = round(nivel * (1 + tp_max_pct / 100), 2) if direccion == "LONG" else round(nivel * (1 - tp_max_pct / 100), 2)

    ultimo_evento_idx = eventos[-1]
    velas_desde_ultimo = len(df) - 1 - ultimo_evento_idx
    duracion_media_min = (df["close_time"] - df["open_time"]).dt.total_seconds().mean() / 60
    horas_desde_ultimo = round(velas_desde_ultimo * duracion_media_min / 60, 1)

    ultima_pct = reacciones[-1]["pct"]
    if ultima_pct >= 1.0:
        tipo_reaccion = "Rechazo de Mecha Violento"
    elif ultima_pct >= min_move_pct:
        tipo_reaccion = "Rebote Moderado"
    else:
        tipo_reaccion = "Reacción Débil"

    return {
        "testeos_previos": len(reacciones),
        "ultima_reaccion_velas": velas_desde_ultimo,
        "ultima_reaccion_horas": f"{horas_desde_ultimo}h",
        "tipo_reaccion": tipo_reaccion,
        "tp_promedio": tp_prom_price,
        "tp_promedio_porcentaje": tp_prom_pct,
        "tp_maximo": tp_max_price,
        "tp_maximo_porcentaje": tp_max_pct,
        "efectividad_zona": win_rate,
        "efectividad_texto": f"{win_rate}% Win Rate histórico en este nivel ({len(reacciones)} testeos)",
    }
