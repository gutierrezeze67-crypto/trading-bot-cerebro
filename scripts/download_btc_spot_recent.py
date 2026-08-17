"""Descarga klines BTCUSDT 1m reales de Binance SPOT (no Futures) -- misma
fuente/mercado que los 11 CSV mensuales ya usados por backtest_htf.py
(C:\\Users\\ezequiel\\Desktop\\bitcoin_data\\spot\\monthly\\klines\\BTCUSDT\\1m\\),
para no mezclar spot historico con futures nuevo (basis real entre ambos
mercados, no son intercambiables sin mas).

Fuente: https://api.binance.com/api/v3/klines (REST publico, sin API key).
Mismas 12 columnas de klines que Binance Futures, mismo formato que
src/orderflow_data.py::KLINE_COLUMNS.

Uso:
    python scripts/download_btc_spot_recent.py --months 6
"""
from __future__ import annotations

import argparse
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
OUT_PATH = REPO_ROOT / "data" / "btcusdt_spot_1m_recent.parquet"
BASE_URL = "https://api.binance.com/api/v3/klines"
SYMBOL = "BTCUSDT"
INTERVAL = "1m"
LIMIT = 1000  # limite real de Binance Spot (distinto de Futures, que permite 1500)
SLEEP_BETWEEN_REQUESTS_S = 0.15
MAX_RETRIES = 5


def _fetch_batch(start_ms: int, end_ms: int, retries: int = MAX_RETRIES) -> list:
    params = {"symbol": SYMBOL, "interval": INTERVAL, "startTime": start_ms, "endTime": end_ms, "limit": LIMIT}
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(BASE_URL, params=params, timeout=15)
            resp.raise_for_status()
            return resp.json()
        except (requests.RequestException, ValueError) as e:
            wait = min(2 ** attempt, 30)
            print(f"  reintento {attempt}/{retries} tras error ({e}) -- esperando {wait}s")
            time.sleep(wait)
    raise SystemExit(f"Fallaron {retries} reintentos consecutivos contra Binance Spot API -- abortando.")


def download(months: int) -> pd.DataFrame:
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=months * 30)

    existing = None
    if OUT_PATH.exists():
        existing = pd.read_parquet(OUT_PATH)
        if not existing.empty:
            last_ts = existing["open_time"].max()
            resume_start = last_ts + timedelta(milliseconds=1)
            print(f"Parquet existente encontrado ({len(existing)} filas, hasta {last_ts}) -- resumiendo desde ahi.")
            resume_start_dt = resume_start.to_pydatetime().replace(tzinfo=timezone.utc)
            start_dt = max(start_dt, resume_start_dt)

    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)
    if start_ms >= end_ms:
        print("Nada nuevo que descargar -- el parquet ya esta al dia.")
        return existing if existing is not None else pd.DataFrame()

    all_rows: list = []
    cursor = start_ms
    batch_span_ms = LIMIT * 60_000
    n_batches = 0

    print(f"Descargando {SYMBOL} SPOT {INTERVAL} desde {start_dt.isoformat()} hasta {end_dt.isoformat()}...")
    while cursor < end_ms:
        batch_end = min(cursor + batch_span_ms, end_ms)
        rows = _fetch_batch(cursor, batch_end)
        if not rows:
            cursor = batch_end
            continue
        all_rows.extend(rows)
        n_batches += 1
        last_open_time = rows[-1][0]
        cursor = last_open_time + 60_000
        if n_batches % 30 == 0:
            print(f"  {n_batches} requests | {len(all_rows)} velas acumuladas | hasta {datetime.fromtimestamp(last_open_time/1000, tz=timezone.utc)}")
        time.sleep(SLEEP_BETWEEN_REQUESTS_S)

    if not all_rows:
        print("No se descargaron velas nuevas.")
        return existing if existing is not None else pd.DataFrame()

    df_new = pd.DataFrame(all_rows, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_volume", "num_trades",
        "taker_buy_base_vol", "taker_buy_quote_vol", "ignore",
    ])
    numeric_cols = ["open", "high", "low", "close", "volume", "quote_volume",
                     "num_trades", "taker_buy_base_vol", "taker_buy_quote_vol"]
    for col in numeric_cols:
        df_new[col] = pd.to_numeric(df_new[col], errors="coerce")
    df_new["open_time"] = pd.to_datetime(df_new["open_time"], unit="ms", utc=True).dt.tz_localize(None)
    df_new["close_time"] = pd.to_datetime(df_new["close_time"], unit="ms", utc=True).dt.tz_localize(None)
    df_new = df_new.dropna(subset=["open_time", *numeric_cols])

    if existing is not None and not existing.empty:
        df_final = pd.concat([existing, df_new], ignore_index=True)
        df_final = df_final.drop_duplicates(subset="open_time").sort_values("open_time").reset_index(drop=True)
    else:
        df_final = df_new.sort_values("open_time").reset_index(drop=True)
    return df_final


def main() -> None:
    parser = argparse.ArgumentParser(description="Descarga klines BTCUSDT 1m reales de Binance SPOT")
    parser.add_argument("--months", type=int, default=6)
    args = parser.parse_args()

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df = download(args.months)
    if df.empty:
        raise SystemExit("Descarga vacia -- revisar conectividad con api.binance.com.")

    df.to_parquet(OUT_PATH, index=False)
    size_mb = OUT_PATH.stat().st_size / (1024 * 1024)
    print(f"\nGuardado en {OUT_PATH}")
    print(f"  filas: {len(df)}")
    print(f"  rango: {df['open_time'].min()} -> {df['open_time'].max()}")
    print(f"  tamano: {size_mb:.1f} MB")


if __name__ == "__main__":
    main()
