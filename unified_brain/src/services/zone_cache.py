"""Cache en memoria, thread-safe, de zonas Sensei -- SIN logica de trading.
API sincrona pensada para consultarse desde SwingExpert.analyze() (que corre
en asyncio.to_thread, ver signal_orchestrator.py), no async a proposito: no
hay I/O real aca, solo un dict protegido por lock.

Estructura interna: dict[symbol] -> dict[zone_id] -> SenseiZone (antes era
dict[symbol] -> list[SenseiZone], con upsert_zone/mark_touch/mark_broken_by_id
haciendo scan lineal por zone_id en cada llamada -- perfilado real corriendo
scripts/validate_sensei_m5.py sobre 30k barras: upsert_zone se comia 5.3 de
13.2s totales, porque la lista de zonas de un symbol solo crece (nada llama
cleanup_old() en el loop de validacion batch, a diferencia de produccion,
donde SignalOrchestrator.process_market_tick() SI lo llama cada tick). Con
dict por zone_id, upsert/touch/mark_broken_by_id son O(1) sin depender de
que alguien limpie a tiempo.

SwingExpert.analyze() todavia no consulta esta cache -- ver la decision de
la conversacion: no se toca swing_expert.py (PF 2.65-3.19 ya validado) hasta
tener un backtest_fused-style A/B que confirme que la confluencia mejora o
mantiene esas metricas.
"""
from __future__ import annotations

import threading

from src.schemas.sensei_zones import SenseiZone


class ZoneCache:
    def __init__(self, max_age_bars: int = 50, bar_minutes: int = 15) -> None:
        self._lock = threading.RLock()
        self._zones: dict[str, dict[str, SenseiZone]] = {}
        self._max_age_ms = max_age_bars * bar_minutes * 60 * 1000

    def upsert_zone(self, zone: SenseiZone) -> None:
        """Upsert real O(1): si zone_id ya existe lo REEMPLAZA (probability/
        mitigation/etc. cambian bar a bar), si no existe lo agrega."""
        with self._lock:
            self._zones.setdefault(zone.symbol, {})[zone.zone_id] = zone

    def get_all_zones(self, symbol: str) -> list[SenseiZone]:
        """Todas las zonas del symbol (rotas incluidas) -- para debug/export
        (ver scripts/validate_sensei_m5.py). get_active_zones() filtra por
        side/precio/mitigation, que no es lo que un checkpoint de validacion
        necesita."""
        with self._lock:
            return list(self._zones.get(symbol, {}).values())

    def get_active_zones(self, symbol: str, side: str, price: float, tick_size: float) -> list[SenseiZone]:
        """Zonas del symbol/side dadas, con precio dentro de [low-2ticks, high+2ticks],
        no rotas y no totalmente mitigadas."""
        with self._lock:
            buffer = 2 * tick_size
            return [
                z for z in self._zones.get(symbol, {}).values()
                if z.side == side and not z.is_broken and z.mitigation != "FULL"
                and (z.price_low - buffer) <= price <= (z.price_high + buffer)
            ]

    def mark_touch(self, symbol: str, zone_id: str, now_ms: int) -> None:
        with self._lock:
            z = self._zones.get(symbol, {}).get(zone_id)
            if z is None:
                return
            z.touch_count += 1
            z.last_touch_ms = now_ms
            if z.mitigation == "UNTESTED":
                z.mitigation = "PARTIAL"

    def mark_broken_by_id(self, symbol: str, zone_id: str) -> None:
        """Invalidacion precisa por zone_id -- a diferencia de mark_broken()
        (que re-deriva la ruptura por precio/side y puede alcanzar a otras
        zonas del mismo lado que cumplan la misma condicion por casualidad),
        esta rompe EXACTAMENTE la zona que el llamador ya identifico como
        rota. Usado por SenseiZoneGenerator, que sabe con certeza cual OB
        se invalido."""
        with self._lock:
            z = self._zones.get(symbol, {}).get(zone_id)
            if z is None:
                return
            z.is_broken = True
            z.mitigation = "FULL"

    def mark_broken(self, symbol: str, price: float, side: str, atr: float) -> None:
        with self._lock:
            for z in self._zones.get(symbol, {}).values():
                if z.side != side or z.is_broken:
                    continue
                broke = (side == "BULLISH" and price < z.price_low - atr) or \
                        (side == "BEARISH" and price > z.price_high + atr)
                if broke:
                    z.is_broken = True
                    z.mitigation = "FULL"

    def cleanup_old(self, now_ms: int) -> None:
        with self._lock:
            for symbol, zones in list(self._zones.items()):
                self._zones[symbol] = {
                    zid: z for zid, z in zones.items()
                    if not z.is_broken and (now_ms - z.ts_ms) < self._max_age_ms
                }
