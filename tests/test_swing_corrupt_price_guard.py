"""Regresion 2026-09-18: el swing abrio un BUY con SL a 9% y TP a 18% porque el
ATR14(15m) del engine valia ~$6,069 en vez de ~$250 (un trade con precio 0 dejo
una vela con low=0). Estos tests fijan las dos defensas: el engine descarta
precios <= 0 y valida el primer trade en vivo contra el backfill; SwingExpert
no opera con un ATR imposible."""
from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace
from unittest.mock import patch

from src.experts.swing_expert import MAX_ATR_PCT, SwingExpert
from src.snapshot_engine import SnapshotEngine


def _trade(price: float, ts: float) -> dict:
    return {"p": str(price), "q": "0.5", "m": False, "T": int(ts * 1000)}


def test_primer_trade_con_precio_cero_se_descarta():
    e = SnapshotEngine(symbol="BTCUSDT")
    t0 = time.time()
    e._handle_trade(_trade(0.0, t0))
    e._handle_trade(_trade(81000.0, t0 + 0.5))
    vela = e._vela_actual
    assert vela["low"] == 81000.0
    assert vela["high"] == 81000.0
    assert e._ultimo_precio_valido == 81000.0


def test_precio_negativo_se_descarta():
    e = SnapshotEngine(symbol="BTCUSDT")
    e._handle_trade(_trade(-5.0, time.time()))
    assert e._vela_actual["open_time"] is None


def test_backfill_siembra_el_ultimo_precio_valido():
    ahora_ms = int(time.time() * 1000)
    klines = []
    for i in range(20):
        open_ms = ahora_ms - (25 - i) * 60_000
        p = 81000.0 + i
        klines.append([open_ms, str(p), str(p + 5), str(p - 5), str(p), "10", open_ms + 59_999, "0", 0, "5", "0", "0"])

    class _Resp:
        def json(self):
            return klines

    e = SnapshotEngine(symbol="BTCUSDT")
    with patch("src.snapshot_engine.requests.get", return_value=_Resp()):
        asyncio.run(e._backfill_klines_1m())

    assert e._ultimo_precio_valido == 81019.0
    # con la semilla, un primer trade en vivo corrupto (>10%) ya no entra
    e._handle_trade(_trade(1.0, time.time()))
    assert e._ultimo_precio_valido == 81019.0


def _snapshot(atr: float | None, close: float):
    return SimpleNamespace(htf_vela={"close": close}, htf_context={"atr14_15m": atr}, ts_ms=0)


def test_swing_no_opera_con_atr_absurdo():
    expert = SwingExpert()
    with patch.object(expert.brain, "decide", side_effect=AssertionError("no debe llegar a decide()")):
        assert expert.analyze(_snapshot(6069.0, 81033.0)) is None
    assert expert.brain.last_reject_reason.startswith("ATR_ABSURDO")


def test_swing_con_atr_normal_sigue_llamando_a_decide():
    expert = SwingExpert()
    llamadas = []
    with patch.object(expert.brain, "decide", side_effect=lambda *a: llamadas.append(a) or None):
        expert.analyze(_snapshot(250.0, 81033.0))
    assert len(llamadas) == 1


def test_tope_no_bloquea_el_maximo_historico_real():
    # maximo real de ATR14(15m)/precio en BTC desde 2026-02: 2.09%
    assert 0.0209 < MAX_ATR_PCT
