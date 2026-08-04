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

def _volume_profile_core(
    lows: np.ndarray, highs: np.ndarray, closes: np.ndarray, vols: np.ndarray,
    bins: int = 50, value_area_pct: float = 0.70,
) -> dict:
    """Nucleo numerico de volume_profile() -- arrays numpy directos, SIN
    pasar por un DataFrame. Extraido de volume_profile() (misma formula,
    cero cambios de comportamiento) para poder llamarse desde un hot path
    bar-a-bar (OrderFlowEngine._update_footprint, Fase 1) sin el overhead de
    pandas por llamada: perfilado real, construir+indexar un DataFrame de
    ~30 filas cuesta ~8ms/llamada (92 barras/s sobre 3000 barras reales) --
    inviable para procesar un CSV de decenas de miles de velas. Con arrays
    numpy directos el mismo calculo es ~2 ordenes de magnitud mas rapido."""
    if len(closes) == 0:
        return {"poc": 0.0, "vah": 0.0, "val": 0.0}

    # Filtro de outliers -- ver docstring de volume_profile() para el porque.
    mediana = float(np.median(closes))
    banda = mediana * 0.15
    mask = (lows >= mediana - banda) & (highs <= mediana + banda)
    if mask.any():
        lows, highs, closes, vols = lows[mask], highs[mask], closes[mask], vols[mask]

    lo, hi = float(lows.min()), float(highs.max())
    if hi <= lo:
        precio = float(closes[-1])
        return {"poc": precio, "vah": precio, "val": precio}

    edges = np.linspace(lo, hi, bins + 1)
    vol_by_bin = np.zeros(bins)

    # searchsorted VECTORIZADO (un solo call para todas las filas, no uno
    # por fila) + acumulacion sobre listas de Python planas (evita el
    # overhead de escalares numpy en el loop) -- mismo resultado que la
    # version fila-a-fila, perfilado real: esto es lo que hace viable llamar
    # esto una vez POR BARRA desde OrderFlowEngine.on_bar_close(), no solo
    # una vez cada 5 minutos como el HTF de SnapshotEngine.
    valid = (highs > lows) & (vols > 0)
    if valid.any():
        i0_arr = np.clip(np.searchsorted(edges, lows[valid], side="right") - 1, 0, bins - 1)
        i1_arr = np.clip(np.searchsorted(edges, highs[valid], side="right") - 1, 0, bins - 1)
        for i0, i1, vol in zip(i0_arr.tolist(), i1_arr.tolist(), vols[valid].tolist()):
            span = i1 - i0 + 1
            vol_by_bin[i0:i1 + 1] += vol / span

    poc_idx = int(np.argmax(vol_by_bin))
    poc = float((edges[poc_idx] + edges[poc_idx + 1]) / 2)

    # Value Area: se expande de a un bin por vez, SIEMPRE contiguo al POC
    # (agrega el vecino - izquierdo o derecho, el que tenga más volumen - de
    # la banda ya cubierta), no "los N bins con más volumen de todo el
    # dataset". La versión anterior rankeaba por volumen global: si había un
    # segundo cluster de volumen lejos del POC (o un solo outlier por un
    # precio corrupto puntual), esos bins podían colarse en la Value Area y
    # producir un VAL/VAH completamente desconectado del rango real de
    # precios operados (visto en vivo: VAL=1283 con POC=63520).
    total_vol = float(vol_by_bin.sum())
    target = total_vol * value_area_pct
    acc = float(vol_by_bin[poc_idx])
    lo_idx = hi_idx = poc_idx
    while acc < target and (lo_idx > 0 or hi_idx < bins - 1):
        vol_izq = float(vol_by_bin[lo_idx - 1]) if lo_idx > 0 else -1.0
        vol_der = float(vol_by_bin[hi_idx + 1]) if hi_idx < bins - 1 else -1.0
        if vol_izq < 0 and vol_der < 0:
            break
        if vol_der >= vol_izq:
            hi_idx += 1
            acc += vol_by_bin[hi_idx]
        else:
            lo_idx -= 1
            acc += vol_by_bin[lo_idx]

    val = float(edges[lo_idx])
    vah = float(edges[hi_idx + 1])

    return {"poc": round(poc, 2), "vah": round(vah, 2), "val": round(val, 2)}


def volume_profile(df: pd.DataFrame, bins: int = 50, value_area_pct: float = 0.70) -> dict:
    if df.empty:
        return {"poc": 0.0, "vah": 0.0, "val": 0.0}
    return _volume_profile_core(
        df["low"].to_numpy(), df["high"].to_numpy(), df["close"].to_numpy(), df["volume"].to_numpy(),
        bins=bins, value_area_pct=value_area_pct,
    )


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


def volumen_percentil(volumen: float, ventana_volumenes: list[float]) -> float:
    """Percentil (0-100) de `volumen` contra una ventana reciente de volúmenes.
    Extraído de SnapshotEngine._volumen_percentil_2h() -- misma fórmula, sin
    depender de self.klines_1m (recibe la ventana ya armada por el llamador,
    para poder reusarse desde cualquier motor, no solo SnapshotEngine)."""
    if len(ventana_volumenes) < 10:
        return 0.0
    ordenados = sorted(ventana_volumenes)
    menor_o_igual = sum(1 for v in ordenados if v <= volumen)
    return (menor_o_igual / len(ordenados)) * 100


def evaluar_absorcion(
    volume: float, close: float, rango: float, delta: float,
    ventana_volumenes: list[float], percentile_threshold: float, flip_ratio_threshold: float,
) -> bool:
    """Absorción: volumen en percentil alto de la ventana reciente + flip
    ratio (proporción del volumen en la dirección contraria al cierre) +
    rango chico pese al volumen (proxy de "price stall", no tick data real).
    Extraído de SnapshotEngine._evaluar_absorcion() -- MISMA fórmula, umbrales
    como parámetros en vez de leer config.constants directo (este módulo no
    depende de config, ver docstring del archivo)."""
    if rango <= 0 or volume <= 0:
        return False
    if volumen_percentil(volume, ventana_volumenes) < percentile_threshold:
        return False
    flip_ratio = abs(delta) / volume
    if flip_ratio <= flip_ratio_threshold:
        return False
    if rango >= close * 0.001:
        return False
    return True


def evaluar_stop_run(high: float, low: float, close: float, delta: float, high_n: float, low_n: float) -> Optional[str]:
    """Stop Run / Liquidity Sweep: la vela barre (wick) el high/low de una
    ventana previa pero CIERRA de vuelta adentro (reclaim), con delta a favor
    de la reversión. Extraído de SnapshotEngine._evaluar_stop_run() --
    high_n/low_n son el high/low de la ventana previa, ya calculados por el
    llamador (antes de incluir la vela actual, sin look-ahead)."""
    if high_n is None or low_n is None:
        return None
    if high > high_n and close <= high_n and delta < 0:
        return "SHORT"
    if low < low_n and close >= low_n and delta > 0:
        return "LONG"
    return None


def evaluar_breakout_vol(
    close: float, delta: float, volume: float,
    range_high_n: float, range_low_n: float, vol_prom_n: float,
    range_pct_threshold: float = 0.003, vol_mult_threshold: float = 2.0,
) -> Optional[str]:
    """Ruptura con volumen: consolidación previa (rango chico) + esta vela
    rompe con volumen >= vol_mult_threshold x el promedio + delta a favor de
    la dirección de la ruptura. Extraído de SnapshotEngine._evaluar_breakout_vol()
    -- misma fórmula, sin el filtro de imbalance por nivel (no calculable sin
    tick data, ver docstring del módulo)."""
    if range_high_n is None or range_low_n is None or close <= 0:
        return None
    rango_pct = (range_high_n - range_low_n) / close
    if rango_pct > range_pct_threshold:
        return None
    if not vol_prom_n or vol_prom_n <= 0 or volume < vol_mult_threshold * vol_prom_n:
        return None
    if delta == 0:
        return None
    return "LONG" if delta > 0 else "SHORT"


def evaluar_exhaustion(
    closes: list, deltas: list, volumes: list,
    delta_divergence_pct: float = 0.7, volume_decay_pct: float = 0.5,
) -> Optional[str]:
    """Agotamiento: la vela actual (ultimo elemento de cada lista, alineadas
    por indice) hace un nuevo extremo de precio sobre la ventana previa
    (closes[:-1]) pero con delta en esa direccion muy por debajo del
    promedio reciente en la misma direccion (divergencia) Y volumen
    decayendo vs el promedio reciente -- señal de que el impulso se esta
    quedando sin combustible, NO una reversion confirmada (eso lo decide
    quien consuma esto, ej. OrderFlowEngine._detectar_exhaustion).

    direction devuelto: "SHORT" (nuevo high con compradores agotandose,
    posible giro bajista) o "LONG" (nuevo low con vendedores agotandose,
    posible giro alcista). None si no hay suficiente historia o ninguna de
    las dos condiciones califica. Mismo patron que evaluar_stop_run/
    evaluar_breakout_vol: funcion pura, umbrales como parametros explicitos,
    sin depender de config."""
    n = len(closes)
    if n < 4 or len(deltas) != n or len(volumes) != n:
        return None

    close, delta, volume = closes[-1], deltas[-1], volumes[-1]
    prev_closes, prev_deltas, prev_volumes = closes[:-1], deltas[:-1], volumes[:-1]

    avg_volume = sum(prev_volumes) / len(prev_volumes)
    if avg_volume <= 0 or volume > avg_volume * (1.0 - volume_decay_pct):
        return None

    if close > max(prev_closes) and delta > 0:
        pos_deltas = [d for d in prev_deltas if d > 0]
        avg_pos_delta = sum(pos_deltas) / len(pos_deltas) if pos_deltas else 0.0
        if avg_pos_delta > 0 and delta <= avg_pos_delta * (1.0 - delta_divergence_pct):
            return "SHORT"

    if close < min(prev_closes) and delta < 0:
        neg_deltas = [d for d in prev_deltas if d < 0]
        avg_neg_delta = sum(neg_deltas) / len(neg_deltas) if neg_deltas else 0.0
        if avg_neg_delta < 0 and delta >= avg_neg_delta * (1.0 - delta_divergence_pct):
            return "LONG"

    return None


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
