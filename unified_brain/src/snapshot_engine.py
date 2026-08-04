"""
Motor de snapshot en tiempo real vía WebSocket de Binance Futures. Reemplaza
los CSV históricos de orderflow_data.py con datos reales de mercado en vivo:
trades (tape/delta/CVD), profundidad L2 (bookmap/iceberg/imbalances reales) y
klines confirmadas (footprint/volume profile HTF).

A diferencia del pipeline histórico (orderflow_data.py), acá SÍ hay datos
trade-by-trade y de order book reales: imbalances por nivel de precio,
iceberg y bookmap dejan de ser aproximaciones - son cálculos genuinos sobre
el book real, no estimaciones.

Usa el endpoint de streams combinados de Binance (una sola conexión para
trade + depth + kline) en vez de 3 conexiones separadas: mismo dato, un solo
loop de reconexión que mantener.

Uso:
    engine = SnapshotEngine("BTCUSDT")
    await engine.start()
    ...
    snapshot = engine.get_live_snapshot()
    ...
    await engine.stop()
"""
import asyncio
import logging
import time
from collections import deque
from datetime import datetime, timezone
from typing import Optional

import numpy as np
import orjson
import pandas as pd
import requests
import websockets

from src import orderflow_data as ofd
from config import constants as c

logger = logging.getLogger(__name__)

COMBINED_WS_URL_TMPL = "wss://fstream.binance.com/stream?streams={streams}"
DEPTH_REST_URL_TMPL = "https://fapi.binance.com/fapi/v1/depth?symbol={symbol}&limit=1000"
KLINES_REST_URL_TMPL = "https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval=1m&limit={limit}"

MAX_TRADES_BUFFER = 20_000
MAX_KLINES_1M = 250  # suficiente para resamplear ~80 velas de 3m
HTF_RECALC_SECONDS = 300
IMBALANCE_RATIO = c.IMBALANCE_RATIO

# Para brain_htf_funding.HTFFundingBrain.decide(), que necesita atr14_15m y
# vwap_15m en PRECIO directo (no en ticks, a diferencia de atr_30m -- ver
# _recompute_atr_30m). No hay equivalentes en config/constants.py (ese
# archivo es de order_flow_signal.py/decide(), no se toca aca).
ATR14_15M_PERIODS = 14
VWAP_15M_LOOKBACK_BARS = 96  # ~24h de velas 15m, ventana rolling (BTC es 24/7, no hay sesion que anclar)


class SnapshotEngine:
    """Mantiene en memoria el estado de mercado en vivo de un símbolo y expone
    get_live_snapshot() con el mismo formato que espera el cerebro (Gemini)."""

    def __init__(self, symbol: str = "BTCUSDT", discord_zones_client=None) -> None:
        self.symbol: str = symbol.upper()
        self.symbol_lower: str = symbol.lower()

        # Opcional - ver src/discord_liquidity_zones.py. Si es None, el
        # detector de LIQUIDITY_POOL simplemente nunca dispara (no rompe nada).
        self._discord_zones = discord_zones_client

        # Order book local (precio -> qty), mantenido vía diff-depth stream
        self.book_bids: dict[float, float] = {}
        self.book_asks: dict[float, float] = {}
        self._book_ready: bool = False
        self._last_update_id: int = 0
        self._primer_evento_pendiente: bool = False

        # Historial de tamaño reciente por nivel de precio (con timestamp), para
        # detectar iceberg v2: ventana de tiempo real, no "últimas N updates"
        self._bid_refresh_history: dict[float, deque] = {}
        self._iceberg_bid_price: float = 0.0
        self._ultimo_iceberg_notificado: float = 0.0
        self._delta_al_marcar_iceberg: dict[float, float] = {}

        # Para la tasa de refresco promedio de la sesión (iceberg v2: hay que
        # comparar contra el promedio, no un umbral fijo)
        self._updates_top5_contador: int = 0
        self._updates_top5_inicio: float = time.time()

        # Trades recientes (para tape_speed): (timestamp_s, price, qty, es_venta_agresiva)
        self.trades: deque = deque(maxlen=MAX_TRADES_BUFFER)

        # Guard-rail de integridad de datos: un precio de trade implausible
        # (glitch puntual del WS, visto en pruebas con desconexiones
        # frecuentes) queda pegado en high/low de la vela y desde ahí
        # arruina el volume profile durante horas (hasta que esa vela sale
        # del buffer) - se descarta el mensaje entero en vez de dejarlo
        # entrar. Ver _handle_trade().
        self._ultimo_precio_valido: float = 0.0

        # Vela 1m en formación. Se cierra sola por tiempo (ver _candle_closer_loop),
        # no depende del stream @kline_1m de Binance (no confiable en pruebas).
        self._vela_actual: dict = self._nueva_vela_acumulador()
        self._vela_cierre_ts: float = 0.0

        # Velas 1m cerradas, listas para armar el snapshot
        self.klines_1m: deque = deque(maxlen=MAX_KLINES_1M)

        # HTF (volume profile) recalculado periódicamente, no en cada trade
        self._htf_cache: dict = {
            "poc": 0.0, "vah": 0.0, "val": 0.0, "cvd_trend": "FLAT",
            "naked_pocs": [], "liq_clusters": {"bids": [], "asks": []},
            "atr_30m": 200.0, "swing_high_h4": None, "swing_low_h4": None,
            "atr14_15m": None, "vwap_15m": None,
        }

        # Filtros institucionales: velas M30/H4 agregadas desde las 1m ya
        # cerradas, para ATR30m (ticks) y swing high/low H4 - ver
        # _recompute_atr_30m / _recompute_swings_h4. Viven en _htf_cache
        # (no en un dict nuevo) para no tocar el esquema de snapshot que ya
        # consumen order_flow_signal.decide() y el resto del pipeline.
        self._candles_30m: deque = deque(maxlen=500)
        self._candles_h4: deque = deque(maxlen=200)
        self._m30_buffer: list = []
        self._h4_buffer: list = []

        # Velas M15 agregadas, solo para HTFFundingBrain.decide() (atr14_15m
        # en precio directo, vwap_15m) -- ver _recompute_atr14_15m /
        # _recompute_vwap_15m. None hasta juntar suficiente historia: mejor
        # eso que un fallback inventado que produzca senales del swing sobre
        # un ATR falso (HTFFundingBrain.decide() ya trata atr14_15m=None como
        # "ATR invalido" y no dispara).
        self._candles_15m: deque = deque(maxlen=500)
        self._m15_buffer: list = []

        # Agregador dedicado a SenseiZoneGenerator (15m/1h/4h/1D), alineado a
        # bordes REALES de reloj UTC (:00/:15/:30/:45 para 15m, etc.) -- a
        # diferencia de _candles_15m/_candles_30m/_candles_h4 de arriba, que
        # agrupan por CANTIDAD de velas 1m cerradas desde que arranco el
        # motor (ya validado asi para ATR30m/swings H4/atr14_15m/vwap_15m,
        # no se toca). Un conteo simple daria velas Sensei desalineadas de
        # las de TradingView -- Pine SIEMPRE alinea a reloj real, y el motor
        # puede arrancar en cualquier momento del dia -- asi que Sensei
        # necesita su propia logica de bucket, no puede reusar la existente.
        # Ver _flush_sensei_bucket().
        self._sensei_tf_seconds = {15: 900, 60: 3600, 240: 14400, 1440: 86400}
        self._sensei_current_bucket: dict[int, dict | None] = {tf: None for tf in self._sensei_tf_seconds}
        self._sensei_candles: dict[int, deque] = {
            15: deque(maxlen=300), 60: deque(maxlen=500), 240: deque(maxlen=500), 1440: deque(maxlen=400),
        }
        self._closed_tfs_this_close: list[int] = []

        # Cola de patrones detectados (absorción/imbalance apilado/iceberg) -
        # se llena al cerrar una vela que califica, no en cada trade (ver
        # _cerrar_vela_actual). El consumidor hace queue.get() bloqueante sin
        # timeout: cero polling, reacciona apenas hay un evento real.
        self.pattern_queue: asyncio.Queue = asyncio.Queue()

        self._tasks: list[asyncio.Task] = []
        self._running: bool = False
        self._ws = None

    # ------------------------------------------------------------------
    # Ciclo de vida
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Lanza las tasks de fondo: WS + reconexión, recálculo HTF y log periódico."""
        if self._running:
            return
        self._running = True
        await self._backfill_klines_1m()
        self._tasks = [
            asyncio.create_task(self._run_forever(), name="ws_loop"),
            asyncio.create_task(self._periodic_htf(), name="htf_loop"),
            asyncio.create_task(self._periodic_log(), name="log_loop"),
            asyncio.create_task(self._candle_closer_loop(), name="candle_closer_loop"),
        ]
        logger.info(f"🚀 SnapshotEngine iniciado para {self.symbol}")

    async def stop(self) -> None:
        """Cierra el WebSocket y cancela todas las tasks de fondo, limpio."""
        self._running = False
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        if self._ws is not None:
            await self._ws.close()
        logger.info("🛑 SnapshotEngine detenido")

    # ------------------------------------------------------------------
    # Conexión WebSocket (streams combinados) + reconexión exponencial
    # ------------------------------------------------------------------

    async def _run_forever(self) -> None:
        # NOTA: el stream @kline_1m de fstream.binance.com no entregó un solo
        # mensaje en pruebas (>2 min conectado, 0 mensajes), mientras que
        # @trade y @depth funcionan normal y el mismo @kline_1m sí funciona en
        # el dominio spot - problema puntual de esa ruta/stream específico, no
        # vale la pena depender de él. Las velas 1m se cierran solas con el
        # timestamp de los trades (ver _candle_closer_loop), que sí es confiable.
        streams = f"{self.symbol_lower}@trade/{self.symbol_lower}@depth@100ms"
        url = COMBINED_WS_URL_TMPL.format(streams=streams)
        backoff = 1

        while self._running:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    self._ws = ws
                    logger.info(f"🔌 Conectado a Binance ({streams})")
                    backoff = 1
                    await self._bootstrap_orderbook()

                    async for raw in ws:
                        if not self._running:
                            break
                        self._procesar_mensaje(orjson.loads(raw))

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"⚠️ WS desconectado ({e}), reintentando en {backoff}s...")
                self._book_ready = False
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    def _procesar_mensaje(self, msg: dict) -> None:
        stream = msg.get("stream", "")
        data = msg.get("data", {})
        if stream.endswith("@trade"):
            self._handle_trade(data)
        elif stream.endswith("@depth@100ms"):
            self._apply_depth_event(data)

    # ------------------------------------------------------------------
    # Order book (bootstrap REST + aplicación de diffs, con detección de gaps)
    # ------------------------------------------------------------------

    async def _backfill_klines_1m(self) -> None:
        """Trae por REST las últimas MAX_KLINES_1M velas 1m ya cerradas
        antes de arrancar el WS en vivo, para que un reinicio del servicio
        no vuelva a pagar el warmup de HTF_READY_MINUTES minutos (ver
        htf_listo()) -- con la estrategia swing-only (scalping
        desactivado) el motor es muy selectivo, perder esa ventana puede
        costar la única señal del día.

        Usa datos reales de Binance (las mismas velas 1m que ya consume
        todo el pipeline), no datos inventados: delta se deriva del taker
        buy/sell volume que ya devuelve el REST de klines (mismo criterio
        agresor/no-agresor que usa el WS de trades, solo que agregado por
        Binance en vez de trade a trade). Lo único que NO se puede
        reconstruir por REST es el detalle por nivel de precio
        (imbalances de book) y iceberg (requieren L2 en vivo) -- quedan
        vacíos/neutros en las velas de backfill: no hay ese dato
        histórico, no se inventa."""
        loop = asyncio.get_event_loop()
        url = KLINES_REST_URL_TMPL.format(symbol=self.symbol, limit=MAX_KLINES_1M)
        try:
            resp = await loop.run_in_executor(None, lambda: requests.get(url, timeout=15))
            raw = resp.json()
        except Exception as e:
            logger.error(f"❌ Error trayendo backfill REST de klines 1m: {e}")
            return

        if not isinstance(raw, list) or not raw:
            logger.warning("⚠️ Backfill REST de klines 1m vacío o inválido, arranca en frío")
            return

        ahora_ms = time.time() * 1000
        for k in raw:
            open_time_ms, o, h, l, c_, vol, close_time_ms, _, _, taker_buy_vol, *_ = k
            if close_time_ms > ahora_ms:
                # Vela todavía en formación en Binance -- no cerrada, no se
                # backfillea (la va a cerrar el WS en vivo apenas arranque).
                continue
            buy_vol = float(taker_buy_vol)
            volume = float(vol)
            vela = {
                "open_time": open_time_ms / 1000,
                "open": float(o), "high": float(h), "low": float(l), "close": float(c_),
                "volume": volume, "buy_vol": buy_vol, "sell_vol": max(volume - buy_vol, 0.0),
                "trades_por_precio": {},
            }
            self._cerrar_vela(vela, es_backfill=True)

        self._recalcular_htf()
        logger.info(
            f"🕯️ Backfill REST completo: {len(self.klines_1m)} velas 1m cargadas "
            f"({'HTF listo' if self.htf_listo() else 'HTF aún no listo'})"
        )

    async def _bootstrap_orderbook(self) -> None:
        """Snapshot REST del book, requerido por Binance antes de aplicar el
        stream de diffs (ver docs de Binance Futures Diff. Depth Stream)."""
        loop = asyncio.get_event_loop()
        url = DEPTH_REST_URL_TMPL.format(symbol=self.symbol)
        try:
            resp = await loop.run_in_executor(None, lambda: requests.get(url, timeout=10))
            data = resp.json()
        except Exception as e:
            logger.error(f"❌ Error obteniendo snapshot del order book: {e}")
            return

        self.book_bids = {float(p): float(q) for p, q in data["bids"] if float(q) > 0}
        self.book_asks = {float(p): float(q) for p, q in data["asks"] if float(q) > 0}
        self._bid_refresh_history.clear()
        self._last_update_id = data["lastUpdateId"]
        self._book_ready = True
        self._primer_evento_pendiente = True
        logger.info(f"📖 Order book resincronizado (lastUpdateId={self._last_update_id})")

    def _apply_depth_event(self, event: dict) -> None:
        if not self._book_ready:
            return

        if self._primer_evento_pendiente:
            # Primer evento tras el snapshot REST: los eventos que llegaron
            # mientras se pedía el snapshot pueden ser anteriores a él (u menor
            # al lastUpdateId) - se descartan sin marcarlos como gap. El primer
            # evento con u >= lastUpdateId es el punto de sincronización real
            # (procedimiento oficial de Binance Diff. Depth Stream); recién de
            # ahí en más se valida la continuidad con "pu".
            if event["u"] < self._last_update_id:
                return
            self._primer_evento_pendiente = False
        else:
            pu = event.get("pu")
            if pu is not None and pu != self._last_update_id:
                logger.warning("⚠️ Gap en el order book (pu != last_update_id) - resincronizando...")
                self._book_ready = False
                asyncio.create_task(self._bootstrap_orderbook())
                return

        for p_str, q_str in event.get("b", []):
            self._update_book_side(self.book_bids, float(p_str), float(q_str), es_bid=True)
        for p_str, q_str in event.get("a", []):
            self._update_book_side(self.book_asks, float(p_str), float(q_str), es_bid=False)

        self._last_update_id = event["u"]

    def _update_book_side(self, book: dict, price: float, qty: float, es_bid: bool) -> None:
        if qty == 0:
            book.pop(price, None)
            if es_bid:
                self._bid_refresh_history.pop(price, None)
                self._delta_al_marcar_iceberg.pop(price, None)
            return

        book[price] = qty
        if not es_bid:
            return

        top_bids = sorted(self.book_bids, reverse=True)[:5]
        if price not in top_bids:
            return

        self._updates_top5_contador += 1

        ahora = time.time()
        hist = self._bid_refresh_history.setdefault(price, deque(maxlen=50))
        hist.append((ahora, qty))
        cutoff = ahora - c.ICEBERG_WINDOW_SECONDS
        while hist and hist[0][0] < cutoff:
            hist.popleft()

        self._evaluar_iceberg(price, hist)

    def _tasa_refresco_promedio(self) -> float:
        """Updates/seg promedio en el TOP 5 del bid desde que arrancó la
        sesión - la referencia contra la que se compara un refresco anómalo."""
        elapsed = time.time() - self._updates_top5_inicio
        if elapsed <= 0:
            return 0.0
        return self._updates_top5_contador / elapsed

    def _precio_consolidando(self, pct: float, segundos: float) -> bool:
        """True si el precio se movió menos de pct% en los últimos `segundos`
        (requisito de iceberg v2: se defiende un nivel en un mercado quieto,
        no en medio de un movimiento fuerte)."""
        ahora = time.time()
        recientes = [t[1] for t in self.trades if ahora - t[0] <= segundos]
        if len(recientes) < 2:
            return False
        rango = max(recientes) - min(recientes)
        precio_ref = recientes[-1]
        return precio_ref > 0 and (rango / precio_ref) < pct

    def _evaluar_iceberg(self, price: float, hist: deque) -> None:
        """Iceberg v2: exige 4 firmas simultáneas, no una sola:
        1) refill real (el tamaño baja por ejecución parcial y vuelve a subir
           cerca de su pico, no cualquier suba aislada);
        2) al menos ICEBERG_MIN_UPDATES refrescos en la ventana de
           ICEBERG_WINDOW_SECONDS;
        3) tasa de refresco por encima del promedio de la sesión (no ruido
           normal del book);
        4) delta expandiendo a favor del iceberg + precio consolidando
           (defendiendo el nivel, no en medio de un impulso)."""
        if len(hist) < c.ICEBERG_MIN_UPDATES:
            return

        qtys = [q for _, q in hist]
        refills = 0
        pico_previo = qtys[0]
        for q in qtys[1:]:
            if q < pico_previo * 0.7:
                pico_previo = q
            elif q >= pico_previo * 0.9:
                refills += 1
                pico_previo = q
        if refills < 1:
            return

        tasa = len(hist) / c.ICEBERG_WINDOW_SECONDS
        if tasa <= self._tasa_refresco_promedio() * 1.2:
            return

        delta_actual = self._vela_actual["buy_vol"] - self._vela_actual["sell_vol"]
        delta_previo = self._delta_al_marcar_iceberg.get(price)
        self._delta_al_marcar_iceberg[price] = delta_actual
        if delta_previo is not None and delta_actual <= delta_previo:
            return

        if not self._precio_consolidando(0.001, 10):
            return

        self._iceberg_bid_price = price

    # ------------------------------------------------------------------
    # Trades -> tape / delta / CVD / imbalances por vela
    # ------------------------------------------------------------------

    @staticmethod
    def _nueva_vela_acumulador() -> dict:
        return {
            # "open" es NUEVO -- el acumulador original nunca lo guardaba
            # (solo high/low/close), porque nada lo necesitaba hasta
            # SenseiZoneGenerator (su deteccion de Order Blocks depende de
            # close[1] < open[1], no se puede portear sin esto).
            "open_time": None, "open": None, "high": None, "low": None, "close": None,
            "volume": 0.0, "buy_vol": 0.0, "sell_vol": 0.0,
            "trades_por_precio": {},  # precio redondeado -> {"buy": x, "sell": y}
        }

    def _handle_trade(self, event: dict) -> None:
        price = float(event["p"])
        qty = float(event["q"])
        es_venta_agresiva = bool(event["m"])  # m=True: comprador es maker -> vendedor agrede
        ts = event["T"] / 1000

        if self._ultimo_precio_valido > 0:
            desviacion = abs(price - self._ultimo_precio_valido) / self._ultimo_precio_valido
            if desviacion > 0.10:
                # >10% de un trade al siguiente es fisicamente imposible en
                # BTCUSDT perpetuo (mercado líquido) - es un dato corrupto,
                # no un movimiento real. Se descarta el mensaje entero. Nivel
                # debug (no warning): confirmado en pruebas que ocurre
                # decenas de veces por corrida - es ruido esperado del feed,
                # no una excepción que valga la pena resaltar en el log.
                logger.debug(
                    f"Trade descartado por precio implausible: {price} "
                    f"(último válido: {self._ultimo_precio_valido}, desviación {desviacion:.1%})"
                )
                return
        self._ultimo_precio_valido = price

        self.trades.append((ts, price, qty, es_venta_agresiva))

        vela = self._vela_actual
        if vela["open_time"] is None:
            vela["open_time"] = ts
            vela["open"] = price
            self._vela_cierre_ts = (int(ts // 60) + 1) * 60  # próximo borde de minuto UTC
        vela["high"] = price if vela["high"] is None else max(vela["high"], price)
        vela["low"] = price if vela["low"] is None else min(vela["low"], price)
        vela["close"] = price
        vela["volume"] += qty

        if es_venta_agresiva:
            vela["sell_vol"] += qty
        else:
            vela["buy_vol"] += qty

        bucket = vela["trades_por_precio"].setdefault(round(price, 2), {"buy": 0.0, "sell": 0.0})
        bucket["sell" if es_venta_agresiva else "buy"] += qty

    async def _candle_closer_loop(self) -> None:
        """Cierra la vela 1m en formación cuando pasa su borde de minuto,
        usando el timestamp de los propios trades (no el stream @kline_1m,
        que no entregó datos en pruebas sobre fstream.binance.com)."""
        while self._running:
            await asyncio.sleep(1)
            if self._vela_actual["open_time"] is None:
                continue
            if time.time() >= self._vela_cierre_ts:
                self._cerrar_vela_actual()

    def _cerrar_vela_actual(self) -> None:
        vela = self._vela_actual
        if vela["open_time"] is None:
            return
        self._cerrar_vela(vela)
        self._vela_actual = self._nueva_vela_acumulador()

    def _cerrar_vela(self, vela: dict, es_backfill: bool = False) -> dict:
        """Construye la vela cerrada (candle dict) a partir de un
        acumulador `vela` (mismo shape que _nueva_vela_acumulador) y la
        enchufa en todo el pipeline: klines_1m, agregación M30/H4/M15,
        Sensei y detección de patrones. Compartido entre el cierre en vivo
        (_cerrar_vela_actual, vela=self._vela_actual) y el backfill REST al
        arrancar (_backfill_klines_1m, vela viene de un kline REST de
        Binance). es_backfill=True evita disparar _evaluar_patron(): una
        vela de hace 3 horas no debe generar una señal de trading ahora."""
        delta = vela["buy_vol"] - vela["sell_vol"]

        imbalances = []
        for price, b in vela["trades_por_precio"].items():
            if b["sell"] > 0 and b["buy"] / b["sell"] >= IMBALANCE_RATIO:
                imbalances.append({"p": price, "r": round(b["buy"] / b["sell"], 2), "s": "bid"})
            elif b["buy"] > 0 and b["sell"] / b["buy"] >= IMBALANCE_RATIO:
                imbalances.append({"p": price, "r": round(b["sell"] / b["buy"], 2), "s": "ask"})
        imbalances.sort(key=lambda x: x["r"], reverse=True)

        rango = (vela["high"] - vela["low"]) if (vela["high"] and vela["low"]) else 0.0
        es_absorcion = self._evaluar_absorcion(vela, delta, rango)

        candle = {
            "t": datetime.fromtimestamp(vela["open_time"], tz=timezone.utc).strftime("%H:%M:%S"),
            "open": round(vela["open"], 2) if vela["open"] is not None else round(vela["close"], 2),
            "c": round(vela["close"], 2),
            "delta": round(delta, 4),
            "vol": round(vela["volume"], 4),
            "imb": imbalances[:5],
            "abs": es_absorcion,
            "iceberg": self._iceberg_bid_price,
            "div": False,
            "_high": vela["high"], "_low": vela["low"], "_open_time": vela["open_time"],
        }
        # Estos se calculan ANTES de appendear la vela (necesitan comparar
        # contra las velas previas, no contra sí misma).
        candle["stop_run"] = self._evaluar_stop_run(candle)
        candle["initiative_pullback"] = self._evaluar_initiative_pullback(candle)
        candle["breakout_vol"] = self._evaluar_breakout_vol(candle)
        candle["liquidity_zone"] = self._evaluar_liquidity_zone(candle)

        self.klines_1m.append(candle)

        # Se resetea en cada cierre de vela 1m -- la gran mayoria de los
        # cierres no cierran ningun TF mayor, asi que esto queda [] casi
        # siempre. Ver bars_multi_tf()/closed_timeframes_this_close() abajo,
        # consumido por MarketSnapshot.from_snapshot_engine() para
        # SenseiZoneGenerator.
        self._closed_tfs_this_close = []

        # Agregación M30 -> H4 para el filtro institucional (ATR30m + swings
        # H4). Se hace acá, no en un loop aparte, para no depender de otro
        # timer más - ya estamos cerrando la vela 1m que dispara todo esto.
        # SIN CAMBIOS -- esto sigue siendo count-based, tal cual estaba
        # validado antes de esta sesion.
        self._m30_buffer.append(candle)
        if len(self._m30_buffer) == 30:
            m30 = self._aggregate_to_m30(self._m30_buffer)
            self._candles_30m.append(m30)
            self._m30_buffer = []
            self._recompute_atr_30m()

            self._h4_buffer.append(m30)
            if len(self._h4_buffer) == 4:
                h4 = self._aggregate_to_h4(self._h4_buffer)
                self._candles_h4.append(h4)
                self._h4_buffer = []
                self._recompute_swings_h4()

        # Agregación M15 -> atr14_15m (precio) + vwap_15m, solo para
        # HTFFundingBrain.decide() (ver comentario de _candles_15m arriba).
        # SIN CAMBIOS -- count-based, validado, no lo toca Sensei.
        self._m15_buffer.append(candle)
        if len(self._m15_buffer) == 15:
            m15 = self._aggregate_to_m30(self._m15_buffer)  # generica, no hardcodea "30" pese al nombre
            self._candles_15m.append(m15)
            self._m15_buffer = []
            self._recompute_atr14_15m()
            self._recompute_vwap_15m()

        # Agregador Sensei, alineado a reloj real (ver _flush_sensei_bucket
        # y el comentario de self._sensei_tf_seconds en __init__) -- TOTALMENTE
        # separado de lo de arriba, para no arriesgar el baseline validado.
        for tf_min, tf_seconds in self._sensei_tf_seconds.items():
            self._flush_sensei_bucket(tf_min, tf_seconds, candle)

        if not es_backfill:
            self._evaluar_patron(candle)
        return candle

    def _flush_sensei_bucket(self, tf_min: int, tf_seconds: int, candle: dict) -> None:
        """Acumula `candle` (vela 1m recien cerrada) en el bucket de TF
        tf_min alineado a reloj real UTC (bucket_start = floor(open_time /
        tf_seconds) * tf_seconds -- mismo criterio que usa cualquier
        exchange/TradingView para sus velas). Si esta vela 1m pertenece a un
        bucket nuevo, el bucket ANTERIOR se cierra y se publica en
        self._sensei_candles[tf_min] -- EXCEPTO la primera vez que se ve un
        bucket para ese TF (arranque del motor, bucket real pero parcial
        porque recien empieza a mitad de ese periodo).

        Version anterior exigia count == cantidad exacta de velas 1m
        esperadas (60 para 1h, 1440 para 1D) para considerar un bucket
        "completo". Sonaba razonable pero en datos reales (reconexiones del
        WS, gaps del feed) casi ningun bucket largo tiene el conteo exacto
        -- confirmado corriendo scripts/validate_sensei_m5.py sobre 2.5 anios
        reales de XAUUSD: CERO velas de 1D se publicaban nunca con ese
        criterio. TradingView tampoco exige un conteo exacto -- agrega lo
        que haya ocurrido en la ventana de reloj, gaps incluidos."""
        ts = candle["_open_time"]
        bucket_start = (ts // tf_seconds) * tf_seconds
        current = self._sensei_current_bucket[tf_min]

        if current is None or current["open_time"] != bucket_start:
            if current is not None and not current["is_first"]:
                self._sensei_candles[tf_min].append(current)
                self._closed_tfs_this_close.append(tf_min)
            self._sensei_current_bucket[tf_min] = {
                "open_time": bucket_start, "open": candle["open"],
                "high": candle["_high"], "low": candle["_low"], "close": candle["c"],
                "volume": candle["vol"], "delta": candle["delta"], "count": 1,
                "is_first": current is None,
            }
            return

        current["high"] = max(current["high"], candle["_high"])
        current["low"] = min(current["low"], candle["_low"])
        current["close"] = candle["c"]
        current["volume"] += candle["vol"]
        current["delta"] += candle["delta"]
        current["count"] += 1

    # ------------------------------------------------------------------
    # Multi-TF para SenseiZoneGenerator -- NUEVO. Copias de solo lectura,
    # nunca los deques vivos del engine.
    # ------------------------------------------------------------------

    def bars_multi_tf(self) -> dict:
        """Ultimas velas OHLC cerradas por timeframe (minutos: 15/60/240/1440),
        alineadas a reloj real UTC (ver _flush_sensei_bucket), listas para
        MarketSnapshot.from_snapshot_engine() -> BarData. Hasta esta version
        no habia forma de sacar velas multi-TF reales del motor -- solo
        escalares agregados (poc/atr14_15m/etc, ver self.htf)."""
        return {tf: list(dq) for tf, dq in self._sensei_candles.items()}

    def closed_timeframes_this_close(self) -> list[int]:
        """Que TFs (15/60/240/1440) cerraron vela en el ULTIMO
        _cerrar_vela_actual() -- vacio la gran mayoria de los cierres."""
        return list(self._closed_tfs_this_close)

    def _evaluar_stop_run(self, candle: dict) -> Optional[str]:
        """Stop Run / Liquidity Sweep v2 -- delega a ofd.evaluar_stop_run()
        (formula real, extraida aca para poder reusarse desde OrderFlowEngine
        sin duplicarla)."""
        previas = list(self.klines_1m)[-20:]
        if len(previas) < 10:
            return None
        high_20 = max(v["_high"] for v in previas if v["_high"] is not None)
        low_20 = min(v["_low"] for v in previas if v["_low"] is not None)
        if candle["_high"] is None or candle["_low"] is None:
            return None
        return ofd.evaluar_stop_run(candle["_high"], candle["_low"], candle["c"], candle["delta"], high_20, low_20)

    def _evaluar_initiative_pullback(self, candle: dict) -> Optional[str]:
        """Pullback a zona de valor HTF (POC/VAL/VAH) con el delta contra-
        tendencia agotándose (volumen chico comparado al promedio reciente) -
        continuación a favor de la tendencia HTF, no una reversión."""
        if not self.htf_listo():
            return None
        htf = self._htf_cache
        poc, vah, val = htf.get("poc"), htf.get("vah"), htf.get("val")
        if not (poc and vah and val):
            return None

        precio = candle["c"]
        if not any(nivel and precio > 0 and abs(precio - nivel) / precio < c.HTF_ZONE_DISTANCE_PCT for nivel in (poc, vah, val)):
            return None

        tendencia = htf.get("cvd_trend")
        if tendencia not in ("UP", "DOWN"):
            return None
        direccion = "LONG" if tendencia == "UP" else "SHORT"

        es_pullback = (direccion == "LONG" and candle["delta"] < 0) or (direccion == "SHORT" and candle["delta"] > 0)
        if not es_pullback:
            return None

        previas = list(self.klines_1m)[-10:]
        if len(previas) < 10:
            return None
        promedio_abs_delta = sum(abs(v["delta"]) for v in previas) / len(previas)
        if promedio_abs_delta <= 0 or abs(candle["delta"]) > 0.3 * promedio_abs_delta:
            return None

        return direccion

    def _evaluar_breakout_vol(self, candle: dict) -> Optional[str]:
        """Consolidación (rango de las últimas 10 velas < 0.3%) + esta vela
        rompe con volumen >= 2x el promedio (core delegado a
        ofd.evaluar_breakout_vol(), reusable desde OrderFlowEngine) + filtro
        EXTRA de imbalance en la dirección de la ruptura -- ese filtro
        necesita datos de tape/L2 reales (candle["imb"]) que solo existen
        acá en vivo, no se movió al módulo compartido."""
        previas = list(self.klines_1m)[-10:]
        if len(previas) < 10:
            return None

        highs = [v["_high"] for v in previas if v["_high"] is not None]
        lows = [v["_low"] for v in previas if v["_low"] is not None]
        if not highs or not lows:
            return None

        vol_prom = sum(v["vol"] for v in previas) / len(previas)
        direccion = ofd.evaluar_breakout_vol(candle["c"], candle["delta"], candle["vol"], max(highs), min(lows), vol_prom)
        if direccion is None:
            return None

        lado_esperado = "bid" if direccion == "LONG" else "ask"
        if not any(imb.get("s") == lado_esperado for imb in candle["imb"]):
            return None
        return direccion

    def _evaluar_liquidity_zone(self, candle: dict) -> Optional[dict]:
        """Zona de liquidez publicada en Discord (texto, canal 'Latin trading
        5.0') que contiene el precio actual. Es un filtro de confluencia, no
        un trigger propio - order_flow_signal.decide() la usa para sumar
        conviction a CUALQUIER setup que coincida en dirección, y como último
        recurso (prioridad más baja) si no hay ningún otro setup de order
        flow activo. Si no hay cliente de Discord inyectado, siempre None -
        no rompe nada, el sistema sigue funcionando igual sin este filtro."""
        if self._discord_zones is None:
            return None
        zona = self._discord_zones.get_zone_at_price(candle["c"])
        if not zona:
            return None
        return {"direction": zona["direction"], "strength": zona.get("strength", 0.0)}

    def _volumen_percentil_2h(self, volumen: float) -> float:
        """Percentil del volumen dado contra las últimas 120 velas 1m (2hs)
        ya cerradas -- delega a ofd.volumen_percentil()."""
        ventana = [v["vol"] for v in list(self.klines_1m)[-120:]]
        return ofd.volumen_percentil(volumen, ventana)

    def _evaluar_absorcion(self, vela: dict, delta: float, rango: float) -> bool:
        """Absorción v2 -- delega a ofd.evaluar_absorcion() (formula real,
        extraida para reusarse desde OrderFlowEngine sin duplicarla)."""
        ventana = [v["vol"] for v in list(self.klines_1m)[-120:]]
        return ofd.evaluar_absorcion(vela["volume"], vela["close"], rango, delta, ventana, c.ABSORPTION_PERCENTILE, c.ABSORPTION_FLIP_RATIO)

    def _evaluar_patron(self, candle: dict) -> None:
        """Si la vela recién cerrada califica como patrón accionable, pushea
        el evento a pattern_queue al instante - no espera al próximo
        recálculo de HTF ni a ningún ciclo. Prioridad (la primera que
        aplica gana, igual que decide() en order_flow_signal.py):
        stop_run > absorción > iceberg > imbalances apilados >
        initiative_pullback / breakout_vol."""
        if candle.get("stop_run"):
            tipo = "stop_run"
        elif candle["abs"]:
            tipo = "absorcion"
        elif candle["iceberg"] and candle["iceberg"] != self._ultimo_iceberg_notificado:
            # self._iceberg_bid_price persiste entre velas una vez detectado -
            # sin este chequeo se dispararía en cada vela para siempre, no
            # solo cuando aparece/cambia.
            tipo = "iceberg"
            self._ultimo_iceberg_notificado = candle["iceberg"]
        elif self._hay_imbalances_apilados():
            tipo = "imbalances_apilados"
        elif candle.get("initiative_pullback"):
            tipo = "initiative_pullback"
        elif candle.get("breakout_vol"):
            tipo = "breakout_vol"
        elif candle.get("liquidity_zone") and candle["liquidity_zone"].get("strength", 0) >= 0.7:
            # Última prioridad, y con una vara más alta (strength >= 0.7):
            # las zonas de Discord son confluencia, no un edge de order flow
            # propio - solo disparan solas si son razonablemente fuertes.
            tipo = "liquidity_pool"
        else:
            return

        evento = {"tipo": tipo, "vela": candle, "snapshot": self.get_live_snapshot()}
        self.pattern_queue.put_nowait(evento)
        logger.info(f"⚡ Patrón detectado: {tipo} @ {candle['c']} - evento en pattern_queue")

    def _hay_imbalances_apilados(self, minimo: int = 3) -> bool:
        """3+ velas seguidas con imbalance en la misma dirección (bid o ask)."""
        ultimas = list(self.klines_1m)[-minimo:]
        if len(ultimas) < minimo:
            return False
        direcciones = []
        for v in ultimas:
            if not v["imb"]:
                return False
            direcciones.append(v["imb"][0]["s"])
        return len(set(direcciones)) == 1

    # ------------------------------------------------------------------
    # HTF (volume profile) - recalculado cada HTF_RECALC_SECONDS, no en cada trade
    # ------------------------------------------------------------------

    async def _periodic_htf(self) -> None:
        while self._running:
            try:
                self._recalcular_htf()
            except Exception as e:
                logger.error(f"❌ Error recalculando HTF: {e}")
            await asyncio.sleep(HTF_RECALC_SECONDS)

    def _recalcular_htf(self) -> None:
        velas = list(self.klines_1m)
        if len(velas) < c.HTF_READY_MINUTES:
            return

        df = pd.DataFrame([{
            "open_time": pd.Timestamp.fromtimestamp(v["_open_time"], tz="UTC"),
            "close_time": pd.Timestamp.fromtimestamp(v["_open_time"], tz="UTC"),
            "high": v["_high"], "low": v["_low"], "close": v["c"],
            "volume": v["vol"], "delta": v["delta"],
        } for v in velas])
        df["cvd"] = df["delta"].cumsum()

        vp = ofd.volume_profile(df)
        self._htf_cache = {
            "poc": vp["poc"], "vah": vp["vah"], "val": vp["val"],
            "cvd_trend": ofd.cvd_trend(df),
            "naked_pocs": ofd.naked_pocs(df),
            "liq_clusters": ofd.liquidity_clusters(df),
            # atr_30m/swings se recalculan aparte, al cerrar M30/H4 (ver
            # _cerrar_vela_actual) - acá solo se preservan, si no este
            # reemplazo cada 5 min los pisaría con el default.
            "atr_30m": self._htf_cache.get("atr_30m", 200.0),
            "swing_high_h4": self._htf_cache.get("swing_high_h4"),
            "swing_low_h4": self._htf_cache.get("swing_low_h4"),
            "atr14_15m": self._htf_cache.get("atr14_15m"),
            "vwap_15m": self._htf_cache.get("vwap_15m"),
        }
        logger.info(f"📊 HTF recalculado: POC={vp['poc']} VAH={vp['vah']} VAL={vp['val']}")

    def _aggregate_to_m30(self, candles_1m: list) -> dict:
        """Agrega N velas 1m cerradas -> 1 vela agregada (generica pese al
        nombre: se usa tanto para M30 -- history original -- como para 1h y
        15m -- ver _cerrar_vela_actual). "open" es el open de la PRIMERA
        vela del grupo (agregado desde candle["open"], ver
        _nueva_vela_acumulador)."""
        return {
            "open_time": candles_1m[-1]["_open_time"],
            "open": candles_1m[0].get("open", candles_1m[0]["c"]),
            "high": max(v["_high"] for v in candles_1m if v["_high"] is not None),
            "low": min(v["_low"] for v in candles_1m if v["_low"] is not None),
            "close": candles_1m[-1]["c"],
            "volume": sum(v["vol"] for v in candles_1m),
            "delta": sum(v["delta"] for v in candles_1m),
        }

    def _aggregate_to_h4(self, candles_m30: list) -> dict:
        """Agrega N velas ya-agregadas (shape de _aggregate_to_m30) -> 1 vela
        de TF mayor. Generica pese al nombre: se usa tanto para 4xM30->H4
        (uso original) como para 6xH4->1D (ver _cerrar_vela_actual)."""
        return {
            "open_time": candles_m30[-1]["open_time"],
            "open": candles_m30[0].get("open", candles_m30[0]["close"]),
            "high": max(v["high"] for v in candles_m30),
            "low": min(v["low"] for v in candles_m30),
            "close": candles_m30[-1]["close"],
            "volume": sum(v["volume"] for v in candles_m30),
            "delta": sum(v["delta"] for v in candles_m30),
        }

    def _recompute_atr_30m(self) -> None:
        """ATR de c.ATR_30M_PERIODS períodos sobre velas M30, en TICKS (no en
        precio) para que decide() lo pueda comparar directo contra distancias
        en ticks sin reconvertir nada."""
        if len(self._candles_30m) < c.ATR_30M_PERIODS + 1:
            self._htf_cache["atr_30m"] = 200.0  # fallback razonable BTC mientras junta historia
            return

        ventana = list(self._candles_30m)[-c.ATR_30M_PERIODS:]
        highs = np.array([v["high"] for v in ventana])
        lows = np.array([v["low"] for v in ventana])
        closes = np.array([v["close"] for v in ventana])
        prev_closes = np.roll(closes, 1)
        prev_closes[0] = closes[0]

        tr = np.maximum(highs - lows, np.maximum(np.abs(highs - prev_closes), np.abs(lows - prev_closes)))
        self._htf_cache["atr_30m"] = float(np.mean(tr) / c.TICK_SIZE)

    def _recompute_swings_h4(self) -> None:
        """Swing High/Low H4 sobre las últimas c.SWING_H4_LOOKBACK velas H4
        (4 días) - zonas adicionales para el filtro de zona institucional."""
        if len(self._candles_h4) < 20:
            return
        lookback = min(c.SWING_H4_LOOKBACK, len(self._candles_h4))
        recientes = list(self._candles_h4)[-lookback:]
        self._htf_cache["swing_high_h4"] = max(v["high"] for v in recientes)
        self._htf_cache["swing_low_h4"] = min(v["low"] for v in recientes)

    def _recompute_atr14_15m(self) -> None:
        """ATR de ATR14_15M_PERIODS sobre velas M15, en PRECIO directo (a
        diferencia de _recompute_atr_30m, que guarda en ticks para
        order_flow_signal.decide()). HTFFundingBrain.decide() hace
        `entry ± atr * sl_atr_mult` directo contra precio, sin reconvertir."""
        if len(self._candles_15m) < ATR14_15M_PERIODS + 1:
            return  # se queda en None (default) hasta juntar historia -- ver comentario en __init__

        ventana = list(self._candles_15m)[-ATR14_15M_PERIODS:]
        highs = np.array([v["high"] for v in ventana])
        lows = np.array([v["low"] for v in ventana])
        closes = np.array([v["close"] for v in ventana])
        prev_closes = np.roll(closes, 1)
        prev_closes[0] = closes[0]

        tr = np.maximum(highs - lows, np.maximum(np.abs(highs - prev_closes), np.abs(lows - prev_closes)))
        self._htf_cache["atr14_15m"] = float(np.mean(tr))

    def _recompute_vwap_15m(self) -> None:
        """VWAP rolling sobre las ultimas VWAP_15M_LOOKBACK_BARS velas M15
        bufferizadas: sum(close*volume)/sum(volume) por vela cerrada -- proxy
        bar-based, no tick-by-tick (mismo criterio que el resto del HTF, que
        trabaja desde klines agregados). BTC es 24/7 (ver ASSET_CONFIG), no
        hay una apertura de sesion natural para anclar un VWAP de sesion."""
        if not self._candles_15m:
            return

        ventana = list(self._candles_15m)[-VWAP_15M_LOOKBACK_BARS:]
        total_vol = sum(v["volume"] for v in ventana)
        if total_vol <= 0:
            return

        self._htf_cache["vwap_15m"] = sum(v["close"] * v["volume"] for v in ventana) / total_vol

    @property
    def htf(self) -> dict:
        """HTF calculado en vivo (puede estar en default hasta juntar ~30 velas
        propias, ver htf_listo())."""
        return self._htf_cache

    def htf_listo(self) -> bool:
        """False mientras el motor no acumuló suficiente historia propia para
        un volume profile con sentido (recién calculado, no en default)."""
        return bool(self._htf_cache.get("poc"))

    # ------------------------------------------------------------------
    # Snapshot público (lo que consume order_flow_signal.py / Gemini)
    # ------------------------------------------------------------------

    def _formatear_velas(self, velas: list[dict]) -> list[dict]:
        limpias = [{
            "t": v["t"], "c": v["c"], "delta": v["delta"], "vol": v["vol"],
            "imb": v["imb"], "abs": v["abs"], "iceberg": v["iceberg"], "div": False,
            "stop_run": v.get("stop_run"), "initiative_pullback": v.get("initiative_pullback"),
            "breakout_vol": v.get("breakout_vol"), "liquidity_zone": v.get("liquidity_zone"),
        } for v in velas]
        ofd._mark_delta_divergence(limpias)
        return limpias

    def _resamplear_a_3m(self) -> list[dict]:
        velas = list(self.klines_1m)
        grupos = []
        for i in range(0, len(velas) - 2, 3):
            grupo = velas[i:i + 3]
            if len(grupo) < 3:
                continue
            delta = sum(v["delta"] for v in grupo)
            vol = sum(v["vol"] for v in grupo)
            imbs = sorted((imb for v in grupo for imb in v["imb"]), key=lambda x: x["r"], reverse=True)
            grupos.append({
                "t": grupo[-1]["t"], "c": grupo[-1]["c"], "delta": round(delta, 4), "vol": round(vol, 4),
                "imb": imbs[:5], "abs": any(v["abs"] for v in grupo), "iceberg": grupo[-1]["iceberg"], "div": False,
            })
        ofd._mark_delta_divergence(grupos)
        return grupos[-20:]

    def _tape_speed(self) -> int:
        """Trades por minuto, ventana de los últimos 10 segundos."""
        ahora = time.time()
        recientes = sum(1 for t in self.trades if ahora - t[0] <= 10)
        return round(recientes / 10 * 60)

    def _bookmap(self) -> dict:
        ask_walls = sorted(self.book_asks.items(), key=lambda x: -x[1])[:5]
        return {
            "bid_iceberg": self._iceberg_bid_price,
            "ask_walls": [[round(p, 2), round(q, 4)] for p, q in ask_walls],
        }

    def get_live_snapshot(self, equity_usdt: float = 50_000, open_positions: Optional[list] = None) -> dict:
        """Devuelve el dict listo para mandarle a Gemini, mismo formato que el
        snapshot histórico de order_flow_signal.build_snapshot(). Incluye
        "htf_ready" explícito para que decide() pueda cortar de una si el
        motor todavía no acumuló los HTF_READY_MINUTES de datos en vivo."""
        return {
            "htf_ready": self.htf_listo(),
            "snapshot": {
                "htf": self._htf_cache,
                "ltf_1m": self._formatear_velas(list(self.klines_1m)[-30:]),
                "ltf_3m": self._resamplear_a_3m(),
                "tape_speed": self._tape_speed(),
                "bookmap": self._bookmap(),
            },
            "equity_usdt": equity_usdt,
            "open_positions": open_positions or [],
        }

    # ------------------------------------------------------------------
    # Diagnóstico
    # ------------------------------------------------------------------

    async def _periodic_log(self) -> None:
        while self._running:
            await asyncio.sleep(30)
            if not self.htf_listo():
                logger.info(f"⏳ HTF warmup: {len(self.klines_1m)}/{c.HTF_READY_MINUTES} min")
            logger.info(
                f"💓 Snapshot engine alive | Trades buffer: {len(self.trades)} | "
                f"Velas 1m: {len(self.klines_1m)} | POC: {self._htf_cache.get('poc', 0)}"
            )


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    for _stream in (sys.stdout, sys.stderr):
        try:
            _stream.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, ValueError):
            pass

    async def _main():
        engine = SnapshotEngine("BTCUSDT")
        await engine.start()
        await asyncio.sleep(30)
        print(engine.get_live_snapshot())
        await engine.stop()

    asyncio.run(_main())
