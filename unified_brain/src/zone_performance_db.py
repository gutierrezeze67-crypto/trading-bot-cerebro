"""
Cache de performance histórica por TIPO de zona institucional (POC, VAH, VAL,
SWING_H4_H, SWING_H4_L, VWAP_15M, EMA21_M5, MID_POC_VAH, MID_POC_VAL) -
calculada por src/historical_analyzer.py sobre los 4 CSV reales de order flow
(src/orderflow_data.py), no sobre datos inventados.

Se agrupa por TIPO de zona, no por precio exacto: el precio de un POC de hace
6 meses no se va a repetir nunca, pero "operar en el POC" como concepto sí es
comparable entre distintos momentos - es lo mismo que hace decide() al usar
_zonas_institucionales()/_satellite_zonas(), cuyos nombres ("POC", "VAH",
"VWAP_15M", etc.) son exactamente las mismas claves que usa este módulo.

CRÍTICO: get_zone_stats() es un lookup 100% en memoria, CERO I/O de disco o
red - decide() la llama en su hot path (<50ms, sin I/O) y una lectura de
disco además de Firestore ya rompió ese objetivo en la práctica (57ms medido
en test_latency.py antes de este fix). El cache se carga UNA SOLA VEZ al
importar el módulo; sync_from_firestore() es opcional y se llama a lo sumo
una vez al arrancar un proceso en vivo (main_deterministic), nunca dentro
del loop de decide()."""
import json
import logging
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_FIRESTORE_PROJECT_ID = "corded-braid-xk8sk"
_COLLECTION = "zone_stats"
_LOCAL_CACHE_PATH = Path(__file__).resolve().parent.parent / "config" / "zone_stats_cache.json"

_client = None
_client_intentado = False


def _get_client():
    global _client, _client_intentado
    if _client is not None or _client_intentado:
        return _client
    _client_intentado = True
    try:
        from google.cloud import firestore
        _client = firestore.Client(project=_FIRESTORE_PROJECT_ID)
        logger.info(f"✅ Cliente de Firestore inicializado para zone_stats (proyecto: {_FIRESTORE_PROJECT_ID})")
    except Exception as e:
        logger.warning(f"⚠️ No se pudo conectar a Firestore para zone_stats: {e}")
        _client = None
    return _client


def _cargar_cache_local() -> dict:
    if _LOCAL_CACHE_PATH.exists():
        try:
            return json.loads(_LOCAL_CACHE_PATH.read_text(encoding="utf-8")).get("zonas", {})
        except Exception as e:
            logger.warning(f"⚠️ No se pudo leer {_LOCAL_CACHE_PATH}: {e}")
    return {}


def _guardar_cache_local() -> None:
    try:
        _LOCAL_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        _LOCAL_CACHE_PATH.write_text(
            json.dumps({"actualizado": time.time(), "zonas": _cache_memoria}, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
    except Exception as e:
        logger.warning(f"⚠️ No se pudo guardar {_LOCAL_CACHE_PATH}: {e}")


# Carga única al importar el módulo - ver nota de CRÍTICO arriba.
_cache_memoria: dict = _cargar_cache_local()


def get_zone_stats(zone_type: str) -> Optional[dict]:
    """Stats agregadas de un TIPO de zona: test_count, win_rate, avg_r_multiple.
    Lookup puro en memoria - None si no hay datos para esta zona (decide()
    debe tratar eso como "sin filtro histórico", nunca como rechazo)."""
    return _cache_memoria.get(zone_type)


def sync_from_firestore() -> int:
    """Refresca el cache en memoria (y el archivo local) desde Firestore.
    Pensado para llamarse una vez al arrancar un proceso en vivo, nunca
    dentro del loop de decide(). Devuelve cuántas zonas se sincronizaron, 0
    si Firestore no está disponible (no bloquea el arranque)."""
    global _cache_memoria
    db = _get_client()
    if db is None:
        return 0
    try:
        docs = db.collection(_COLLECTION).stream()
        nuevas = {d.id: d.to_dict() for d in docs}
        if nuevas:
            _cache_memoria = nuevas
            _guardar_cache_local()
        return len(nuevas)
    except Exception as e:
        logger.warning(f"⚠️ Error sincronizando zone_stats desde Firestore: {e}")
        return 0


def upsert_zone_stats(zone_type: str, stats: dict) -> None:
    """Llamado solo desde historical_analyzer.py (proceso offline), nunca
    desde decide()."""
    global _cache_memoria
    _cache_memoria[zone_type] = {**stats, "zone_type": zone_type, "actualizado": time.time()}
    _guardar_cache_local()

    db = _get_client()
    if db is None:
        return
    try:
        db.collection(_COLLECTION).document(zone_type).set(_cache_memoria[zone_type], merge=True)
    except Exception as e:
        logger.warning(f"⚠️ Error guardando zone_stats para {zone_type} en Firestore: {e}")
