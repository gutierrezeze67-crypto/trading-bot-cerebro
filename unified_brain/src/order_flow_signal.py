"""
Genera señales de trading con el "cerebro" de Order Flow (config/systemInstruction.txt)
usando datos reales de BTCUSDT (ver src/orderflow_data.py para qué es real vs aproximado).

Uso:
    python -m src.order_flow_signal
    python -m src.order_flow_signal --equity 25000
"""
import argparse
import asyncio
import concurrent.futures
import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# En consolas Windows con code page heredado (cp1252/cp850), imprimir emojis
# puede lanzar UnicodeEncodeError y tirar el proceso. Forzamos UTF-8.
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass

from google import genai
from google.genai import types, errors

from config import settings
from config import constants as c
from src import orderflow_data as ofd
from src.notifier import enviar_telegram
from src.zone_performance_db import get_zone_stats

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
SYSTEM_INSTRUCTION_PATH = BASE_DIR / "config" / "systemInstruction.txt"
SIGNALS_HISTORY_PATH = BASE_DIR / "data" / "signals_history.json"
LATEST_SIGNAL_PATH = BASE_DIR / "latest_signal.json"

REQUIRED_FIELDS = [
    "decision", "conviction", "setup_type", "entry_price", "stop_loss_price",
    "position_size_usdt", "tp_levels", "htf_context_1line", "ltf_trigger_detail",
    "confluence_checklist", "risk_metrics", "execution_orders", "management_notes",
]

MODELOS = ["gemini-2.5-flash", "gemini-2.0-flash"]


def load_system_instruction() -> str:
    if not SYSTEM_INSTRUCTION_PATH.exists():
        raise FileNotFoundError(f"No se encontró {SYSTEM_INSTRUCTION_PATH}")
    content = SYSTEM_INSTRUCTION_PATH.read_text(encoding="utf-8")
    if len(content.strip()) < 200:
        raise ValueError(
            f"systemInstruction.txt tiene solo {len(content)} caracteres - parece "
            f"vacío o incompleto ({SYSTEM_INSTRUCTION_PATH})"
        )
    logger.info(f"✅ System Instruction cargada ({len(content)} caracteres)")
    return content


# ----------------------------------------------------------------------
# Snapshot
# ----------------------------------------------------------------------

def build_snapshot(tfs: dict, equity_usdt: float = 50000, open_positions: Optional[list] = None) -> dict:
    if "1h" not in tfs or "1m" not in tfs:
        raise FileNotFoundError(f"No se encontraron los CSV de order flow en {ofd.DATA_DIR}")

    df_htf = tfs["1h"]
    df_1m = tfs["1m"]

    # El M1 es el timeframe más corto y define el "ahora" del snapshot (hoy solo
    # cubre jul-2025 por un bug de exportación - ver orderflow_data.py). Se recorta
    # el HTF al mismo punto para que HTF y LTF describan el mismo momento; si no,
    # el POC/VAH/VAL quedarían calculados con datos de casi un año después de las
    # velas LTF, y la señal no tendría sentido.
    ancla = df_1m["open_time"].max()
    df_htf = df_htf[df_htf["open_time"] <= ancla].reset_index(drop=True)

    dias_de_atraso = (datetime.now() - ancla.to_pydatetime()).days
    if dias_de_atraso > 3:
        logger.warning(
            f"⚠️ Los datos de order flow (M1) llegan hasta {ancla} ({dias_de_atraso} días "
            f"atrás de la fecha actual). Esto es una foto histórica, no un feed en vivo - "
            f"para señales en tiempo real hay que reemplazar orderflow_data.load_symbol() "
            f"por un feed live (ej. Binance WebSocket) o re-exportar el M1 con el año completo."
        )

    vp = ofd.volume_profile(df_htf.tail(200))
    htf = {
        "poc": vp["poc"],
        "vah": vp["vah"],
        "val": vp["val"],
        "cvd_trend": ofd.cvd_trend(df_htf),
        "naked_pocs": ofd.naked_pocs(df_htf),
        "liq_clusters": ofd.liquidity_clusters(df_htf),
    }

    ltf_1m = ofd.build_ltf_candles(df_1m, n=30)
    df_3m = ofd.resample_klines(df_1m, "3min")
    ltf_3m = ofd.build_ltf_candles(df_3m, n=20)

    snapshot = {
        "htf": htf,
        "ltf_1m": ltf_1m,
        "ltf_3m": ltf_3m,
        "tape_speed": ofd.tape_speed(df_1m),
        "bookmap": ofd.empty_bookmap(),
    }

    logger.info(
        f"📊 Snapshot armado: POC={htf['poc']} VAH={htf['vah']} VAL={htf['val']} "
        f"CVD={htf['cvd_trend']} | último precio LTF={ltf_1m[-1]['c'] if ltf_1m else 'N/A'}"
    )

    return {
        "snapshot": snapshot,
        "equity_usdt": equity_usdt,
        "open_positions": open_positions or [],
    }


# ----------------------------------------------------------------------
# Llamada a Gemini
# ----------------------------------------------------------------------

def _clean_json_text(texto: str) -> str:
    texto = texto.strip()
    if texto.startswith("```"):
        texto = texto.split("\n", 1)[1] if "\n" in texto else texto
        if texto.endswith("```"):
            texto = texto[:-3]
        if texto.lower().startswith("json"):
            texto = texto[4:]
    return texto.strip()


def _parsear_signal(texto: str) -> Optional[dict]:
    try:
        signal = json.loads(_clean_json_text(texto))
    except json.JSONDecodeError as e:
        logger.error(f"❌ Error parseando JSON de Gemini: {e}")
        logger.debug(f"Raw (primeros 300 chars): {texto[:300]}")
        return None

    faltantes = [f for f in REQUIRED_FIELDS if f not in signal]
    if faltantes:
        logger.error(f"❌ Señal incompleta, faltan campos: {faltantes}")
        return None

    return signal


def ask_gemini_for_signal(payload: dict, timeout: int = 45, max_retries: int = 3) -> Optional[dict]:
    api_key = getattr(settings, "GEMINI_API_KEY", "") or ""
    if not api_key or "PON_TU" in api_key:
        logger.error("❌ GEMINI_API_KEY no configurada en config/settings.py")
        return None
    if not (api_key.startswith("AIza") or api_key.startswith("AQ.")):
        logger.warning(
            "⚠️ La API Key no tiene ninguno de los prefijos conocidos de Google AI "
            "Studio ('AIzaSy...' o 'AQ...'). Es probable que la llamada falle con "
            "error de autenticación. Generá una clave válida en https://aistudio.google.com/apikey"
        )

    try:
        client = genai.Client(api_key=api_key)
    except Exception as e:
        logger.error(f"❌ No se pudo inicializar el cliente de Gemini: {e}")
        return None

    try:
        system_instruction = load_system_instruction()
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"❌ {e}")
        return None

    config = types.GenerateContentConfig(
        temperature=0.15,
        top_p=0.9,
        top_k=20,
        max_output_tokens=16384,  # 8192 truncó una respuesta real en pruebas (cuenta el "pensamiento" interno, no solo el JSON)
        system_instruction=system_instruction,
        response_mime_type="application/json",
    )

    contenido = json.dumps(payload, ensure_ascii=False)
    ultimo_error = None

    for modelo in MODELOS:
        for intento in range(1, max_retries + 1):
            try:
                logger.info(f"📤 Enviando snapshot a {modelo} (intento {intento}/{max_retries})...")
                respuesta = client.models.generate_content(
                    model=modelo, contents=contenido, config=config,
                )
                texto = getattr(respuesta, "text", None)
                if not texto:
                    logger.error(f"❌ Respuesta sin texto de {modelo}: {respuesta}")
                    return None

                signal = _parsear_signal(texto)
                if signal:
                    logger.info(
                        f"✅ Señal: {signal.get('decision')} | "
                        f"Setup: {signal.get('setup_type')} | "
                        f"Conviction: {signal.get('conviction')}"
                    )
                return signal

            except errors.ClientError as e:
                ultimo_error = e
                if e.code == 404:
                    logger.warning(f"📡 Modelo {modelo} no disponible, probando el siguiente...")
                    break
                elif e.code == 429:
                    logger.warning(f"⏰ Cuota excedida en {modelo} (intento {intento}/{max_retries})")
                    time.sleep(2 ** intento)
                    continue
                elif e.code in (401, 403):
                    logger.error(
                        f"🔑 API Key inválida o sin permisos ({e.code}): {e.message}. "
                        f"Verificá GEMINI_API_KEY en config/settings.py."
                    )
                    return None
                else:
                    logger.error(f"❌ Error de cliente Gemini ({e.code}): {e.message}")
                    return None

            except errors.ServerError as e:
                ultimo_error = e
                logger.warning(f"❌ Error de servidor Gemini ({e.code}), reintentando... (intento {intento}/{max_retries})")
                time.sleep(2 ** intento)

            except Exception as e:
                ultimo_error = e
                logger.error(f"❌ Error inesperado llamando a Gemini: {e}")
                return None

    logger.error(f"❌ No se pudo obtener señal tras agotar reintentos y modelos: {ultimo_error}")
    return None


# ----------------------------------------------------------------------
# Motor de decisión determinista (Python puro, sin Gemini)
# ----------------------------------------------------------------------
# decide() reemplaza el rol decisorio de Gemini: 100% reglas, sin llamadas de
# red, pensado para correr en <50ms. Gemini pasa a validar/narrar DESPUÉS
# (ver gemini_validator.py), nunca antes ni durante.
#
# Setups soportados: los 7 que snapshot_engine.py detecta de verdad
# (STOP_RUN, ABSORPTION, DELTA_DIV, INITIATIVE_PULLBACK, BREAKOUT_VOL,
# IMBALANCE_STACK, ICEBERG). Nada se simula acá - si algún día se agrega un
# detector nuevo, se suma a _prioridad_setup() con su prioridad.

_PRIORIDAD_CONVICTION = {1: 9, 2: 8, 3: 7, 4: 7, 5: 6, 6: 6}  # prioridad -> conviction base


def _no_trade(razon: str) -> dict:
    return {
        "decision": "NO_TRADE", "conviction": 0, "setup_type": "NO_TRADE",
        "entry_price": 0.0, "stop_loss_price": 0.0, "position_size_usdt": 0.0,
        "tp_levels": [], "htf_context_1line": razon, "ltf_trigger_detail": razon,
        "confluence_checklist": {}, "risk_metrics": {"r_multiple_tp1": 0.0, "risk_pct_account": 0.0, "time_stop_min": 0},
        "execution_orders": [], "management_notes": razon,
    }


def _imbalances_apilados(ltf_1m: list, minimo: int = 3) -> bool:
    ultimas = ltf_1m[-minimo:]
    if len(ultimas) < minimo:
        return False
    lados = []
    for v in ultimas:
        if not v.get("imb"):
            return False
        lados.append(v["imb"][0]["s"])
    return len(set(lados)) == 1


def _prioridad_setup(ltf_1m: list) -> tuple:
    """Evalúa la última vela cerrada contra los setups detectables, en orden
    de prioridad (mismo orden que _evaluar_patron en snapshot_engine.py).
    Devuelve (setup, direccion, prioridad) o (None, None, 99)."""
    ultima = ltf_1m[-1]

    if ultima.get("stop_run"):
        return "STOP_RUN", ultima["stop_run"], 1

    if ultima.get("abs"):
        # Absorción: delta negativo absorbido en el nivel -> reversión al alza
        return "ABSORPTION", ("LONG" if ultima["delta"] < 0 else "SHORT"), 2

    if ultima.get("div"):
        return "DELTA_DIV", ("LONG" if ultima["delta"] < 0 else "SHORT"), 3

    if ultima.get("initiative_pullback"):
        return "INITIATIVE_PULLBACK", ultima["initiative_pullback"], 4

    if ultima.get("breakout_vol"):
        return "BREAKOUT_VOL", ultima["breakout_vol"], 4

    if _imbalances_apilados(ltf_1m):
        lado = ultima["imb"][0]["s"]
        return "IMBALANCE_STACK", ("LONG" if lado == "bid" else "SHORT"), 5

    if ultima.get("iceberg"):
        # El detector de iceberg solo trackea el lado bid (ver snapshot_engine.py)
        return "ICEBERG", "LONG", 5

    zona = ultima.get("liquidity_zone")
    if zona and zona.get("direction") in ("LONG", "SHORT"):
        # Última prioridad: confluencia de Discord sin ningún otro setup de
        # order flow detrás - ver filtro de strength>=0.7 en snapshot_engine.py.
        return "LIQUIDITY_POOL", zona["direction"], 6

    return None, None, 99


def _confluencia_htf(precio: float, htf: dict) -> bool:
    if precio <= 0:
        return False
    for nivel in (htf.get("poc"), htf.get("vah"), htf.get("val")):
        if nivel and abs(precio - nivel) / precio <= c.HTF_ZONE_DISTANCE_PCT:
            return True
    return False


def _calcular_niveles(direccion: str, precio: float, htf: dict, ltf_1m: list) -> tuple:
    """TP1/TP2/SL fijos en distancia de precio desde el entry (TP1_PIPS/
    TP2_PIPS/SL_PIPS), no desde la zona HTF ni escalados por ATR - ver nota
    en config/constants.py. Gestión híbrida: TP1 cierra 50% (size_pct=50),
    TP2 es el corredor con el 50% restante (ver _gestionar_bracket)."""
    entry = precio
    if direccion == "LONG":
        sl = entry - c.SL_PIPS
        tp1 = entry + c.TP1_PIPS
        tp2 = entry + c.TP2_PIPS
    else:
        sl = entry + c.SL_PIPS
        tp1 = entry - c.TP1_PIPS
        tp2 = entry - c.TP2_PIPS

    if sl <= 0 or sl == entry:
        return None, None, []

    tps = [
        {"price": round(tp1, 2), "size_pct": 50, "logic": f"TP1 fijo {c.TP1_PIPS:.0f} USDT"},
        {"price": round(tp2, 2), "size_pct": 50, "logic": f"TP2 fijo {c.TP2_PIPS:.0f} USDT (corredor)"},
    ]
    return entry, sl, tps


def _r_multiple(entry: float, sl: float, tp1: float) -> float:
    riesgo = abs(entry - sl)
    return round(abs(tp1 - entry) / riesgo, 2) if riesgo > 0 else 0.0


def _validar_niveles_seguros(direccion: str, entry: float, sl: float, tps: list) -> tuple:
    """Última red de seguridad antes de emitir una señal: rechaza niveles
    absurdos (TP/SL pegados al entry, o un tp2 fuera de cualquier relación
    lógica con tp1) que un dato HTF corrupto puntual podría producir incluso
    después del filtro de sanity en _calcular_niveles. Devuelve
    (valido, motivo)."""
    tp1 = tps[0]["price"]

    if tp1 == entry:
        return False, "TP_ABSURD: TP1 == entry"
    if sl == entry:
        return False, "SL_ABSURD: SL == entry"
    if abs(tp1 - entry) < c.SIGNAL_MIN_TP_DIST_USDT:
        return False, f"TP_TOO_TIGHT: TP1 a {abs(tp1 - entry):.2f} USD (mínimo {c.SIGNAL_MIN_TP_DIST_USDT:.0f})"
    if abs(entry - sl) < c.SIGNAL_MIN_SL_DIST_USDT:
        return False, f"SL_TOO_TIGHT: SL a {abs(entry - sl):.2f} USD (mínimo {c.SIGNAL_MIN_SL_DIST_USDT:.0f})"

    if len(tps) >= 2:
        tp2 = tps[1]["price"]
        orden_ok = (tp2 > tp1) if direccion == "LONG" else (tp2 < tp1)
        if not orden_ok:
            return False, f"TP2_ABSURD: TP2={tp2} no está más allá de TP1={tp1} en la dirección {direccion}"
        dist_tp1 = abs(tp1 - entry)
        if dist_tp1 > 0 and abs(tp2 - entry) > c.TP2_MAX_MULT_OF_TP1 * dist_tp1:
            return False, f"TP2_ABSURD: TP2 a {abs(tp2 - entry):.2f} USD, más de {c.TP2_MAX_MULT_OF_TP1:.0f}x la distancia de TP1 ({dist_tp1:.2f})"

    return True, ""


# Filtros institucionales: reducen ruido operando solo en zonas donde el
# dinero grande decide (POC/VAH/VAL/Swing H4), con cooldown por zona y R:R
# estructural real (ver config/constants.py). decide() es una función suelta
# (no un método de clase), así que el cooldown vive a nivel de módulo - es
# estado real del proceso, se resetea solo si se reinicia el script.
_ZONE_COOLDOWNS: dict = {}  # "POC_64000" -> ts_expiry_ms


def _zonas_institucionales(htf: dict) -> list:
    zonas = [
        ("POC", htf.get("poc")),
        ("VAH", htf.get("vah")),
        ("VAL", htf.get("val")),
        ("SWING_H4_H", htf.get("swing_high_h4")),
        ("SWING_H4_L", htf.get("swing_low_h4")),
    ]
    return [(nombre, nivel) for nombre, nivel in zonas if nivel]


def _is_institutional_zone(precio: float, htf: dict) -> tuple:
    """True si el precio está a <= INSTITUTIONAL_ZONE_TOLERANCE_PCT de
    POC/VAH/VAL/Swing H4 - el filtro más importante: sin esto no importa qué
    patrón se detectó, se está operando en 'aire'. Devuelve también el tipo
    de zona que matcheó (o "" si ninguna), para el filtro histórico."""
    if precio <= 0:
        return False, "FUERA_ZONA_INSTITUCIONAL", ""
    for nombre, nivel in _zonas_institucionales(htf):
        dist_pct = abs(precio - nivel) / precio
        if dist_pct <= c.INSTITUTIONAL_ZONE_TOLERANCE_PCT:
            return True, f"ZONA_INST: {nombre}@{nivel:.1f} (dist={dist_pct:.3%})", nombre
    return False, "FUERA_ZONA_INSTITUCIONAL", ""


def _boost_historico(conviction: int, zone_type: str) -> tuple:
    """Consulta zone_stats (backtest offline sobre datos reales - ver
    src/historical_analyzer.py), agrupado por TIPO de zona, no por precio
    exacto. Sin datos suficientes (test_count < ZONE_HIST_MIN_TESTS) no
    filtra ni bonifica nada - "sin historial" nunca es lo mismo que "zona
    mala". Devuelve (permitido, conviction_ajustada, nota_para_logs)."""
    if not zone_type:
        return True, conviction, ""
    stats = get_zone_stats(zone_type)
    if not stats or stats.get("test_count", 0) < c.ZONE_HIST_MIN_TESTS:
        return True, conviction, ""

    wr = stats.get("win_rate", 0.0)
    avg_r = stats.get("avg_r_multiple", 0.0)
    if wr < c.ZONE_HIST_REJECT_WR:
        return False, conviction, f"histórico de {zone_type} malo (WR={wr:.0%} en {stats['test_count']} tests)"

    boost = 0
    if wr >= c.ZONE_HIST_BOOST2_WR and avg_r > 0:
        boost = 2
    elif wr >= c.ZONE_HIST_BOOST1_WR:
        boost = 1

    nota = f"histórico {zone_type}: WR={wr:.0%} AvgR={avg_r:.2f} ({stats['test_count']} tests)"
    return True, min(conviction + boost, 10), nota


def _check_zone_cooldown(precio: float, htf: dict, ts: int) -> tuple:
    """Bloquea una zona (tolerancia 0.1%, más ajustada que la de detección)
    por ZONE_COOLDOWN_HOURS tras evaluarla - evita re-operar el mismo nivel
    en un chop de pocos minutos."""
    if precio <= 0:
        return True, "Zona sin cooldown"
    for nombre, nivel in _zonas_institucionales(htf):
        if abs(precio - nivel) / precio <= 0.001:
            key = f"{nombre}_{nivel:.0f}"
            expiry = _ZONE_COOLDOWNS.get(key, 0)
            if ts < expiry:
                restante_min = (expiry - ts) / 1000 / 60
                return False, f"COOLDOWN_ZONA {key}: {restante_min:.0f}min restantes"
            _ZONE_COOLDOWNS[key] = ts + c.ZONE_COOLDOWN_HOURS * 3600 * 1000
            return True, f"Zona {nombre} liberada, cooldown {c.ZONE_COOLDOWN_HOURS}h activado"
    return True, "Zona sin cooldown"


def decide(payload: dict, equity: float, risk_manager) -> dict:
    """Motor de decisión determinista: sin Gemini, sin I/O, pensado para
    correr en <50ms. Zonas institucionales (POC/VAH/VAL/Swing H4) + cooldown
    de zona + TP1/TP2/SL fijos en distancia de precio (ver
    config/constants.py) - el R:R queda garantizado por construcción
    (TP1/SL=1:1, TP2/SL=2:1), no se vuelve a validar en runtime. Hubo una
    versión con un segundo motor SATELLITE (micro-zonas, TP rápido) - se
    eliminó por decisión explícita: solo se opera swing institucional."""
    if not payload.get("htf_ready", False):
        return _no_trade("HTF no disponible - esperando datos en vivo suficientes (ver snapshot_engine.py HTF gate)")

    snap = payload["snapshot"]
    htf = snap.get("htf", {})
    ltf_1m = snap.get("ltf_1m") or []
    if not ltf_1m:
        return _no_trade("Sin velas 1m disponibles todavía")

    ultima = ltf_1m[-1]
    precio = ultima["c"]
    ts = int(time.time() * 1000)

    # --- FILTRO INSTITUCIONAL 1: ZONA (POC/VAH/VAL/Swing H4) ---
    # El más importante: sin esto no importa qué patrón se detectó, se está
    # operando en "aire" en vez de en un nivel donde el dinero grande decide.
    en_zona, motivo_zona, zone_type_core = _is_institutional_zone(precio, htf)
    if not en_zona:
        return _no_trade(f"RECHAZADO: {motivo_zona}")

    # --- FILTRO INSTITUCIONAL 2: COOLDOWN ESTRUCTURAL ---
    zona_libre, motivo_cooldown = _check_zone_cooldown(precio, htf, ts)
    if not zona_libre:
        return _no_trade(f"RECHAZADO: {motivo_cooldown}")

    setup, direccion, prioridad = _prioridad_setup(ltf_1m)
    if setup is None:
        return _no_trade("Sin patrón accionable en la última vela")

    if not _confluencia_htf(precio, htf):
        return _no_trade(f"{setup} detectado pero sin confluencia HTF (precio lejos de POC/VAL/VAH)")

    entry, sl, tps = _calcular_niveles(direccion, precio, htf, ltf_1m)
    if entry is None:
        return _no_trade(f"{setup} detectado pero no se pudo calcular un SL válido")

    niveles_ok, motivo_niveles = _validar_niveles_seguros(direccion, entry, sl, tps)
    if not niveles_ok:
        return _no_trade(f"RECHAZADO: {motivo_niveles}")

    conviction = _PRIORIDAD_CONVICTION.get(prioridad, 6)

    # --- FILTRO HISTÓRICO: performance real de este tipo de zona (ver historical_analyzer.py) ---
    permitido_hist, conviction, nota_hist = _boost_historico(conviction, zone_type_core)
    if not permitido_hist:
        return _no_trade(f"RECHAZADO: {nota_hist}")

    zona = ultima.get("liquidity_zone")
    if setup != "LIQUIDITY_POOL" and zona and zona.get("direction") == direccion:
        # Boost de confluencia: una zona de Discord alineada con la dirección
        # del setup de order flow suma conviction. Si el setup YA ES
        # LIQUIDITY_POOL no se vuelve a sumar (sería contarla dos veces).
        conviction += 1
        if zona.get("strength", 0) > 0.8:
            conviction += 1
        conviction = min(conviction, 10)

    size_usdt = risk_manager.calcular_tamano_posicion(conviction, entry, sl, equity)

    checklist = {
        "htf_structure_align": htf.get("cvd_trend") in ("UP", "DOWN"),
        "key_level_test": True,
        "micro_trigger_clear": True,
        "delta_confirmation": bool(ultima.get("delta")),
        "imbalance_stack_dir": _imbalances_apilados(ltf_1m),
        "bookmap_liq_support": bool(ultima.get("iceberg")),
        "session_allowed": True,
    }

    return {
        "decision": direccion,
        "conviction": conviction,
        "setup_type": setup,
        "entry_price": round(entry, 2),
        "stop_loss_price": round(sl, 2),
        "position_size_usdt": size_usdt,
        "tp_levels": tps,
        "htf_context_1line": (
            f"POC={htf.get('poc')} VAH={htf.get('vah')} VAL={htf.get('val')} "
            f"CVD={htf.get('cvd_trend')} - precio {precio} en zona de valor"
        ),
        "ltf_trigger_detail": f"{setup} confirmado en vela {ultima['t']} (delta={ultima['delta']}, vol={ultima['vol']})",
        "confluence_checklist": checklist,
        "risk_metrics": {
            "r_multiple_tp1": _r_multiple(entry, sl, tps[0]["price"]) if tps else 0.0,
            "risk_pct_account": round(c.RISK_POR_CONVICTION.get(conviction, c.RISK_PER_TRADE_BASE) * 100, 3),
            "time_stop_min": c.TIME_STOP_TP1_MINUTES,
        },
        "execution_orders": [],  # sin ejecución real - este proyecto no manda órdenes a ningún exchange
        "management_notes": (
            f"MOVER SL A BE AL TOCAR TP1. CERRAR 50% A LOS {c.TIME_STOP_TP1_MINUTES} MIN SIN TP1. "
            f"CERRAR TODO A LOS {c.TIME_STOP_CLOSE_MINUTES} MIN SIN TP1."
            + (f" | {nota_hist}" if nota_hist else "")
        ),
    }


# ----------------------------------------------------------------------
# Enriquecimiento con datos reales (backtest histórico + microestructura)
# ----------------------------------------------------------------------
# Gemini NO completa analisis_historico/microestructura (ver systemInstruction.txt) -
# se calculan acá con datos reales para no depender de que el LLM invente cifras.

def enrich_signal_with_real_context(signal: dict, tfs: dict) -> dict:
    decision = signal.get("decision")
    signal.setdefault("activo", "BTCUSDT")
    signal.setdefault("timeframe", "15m")
    signal.setdefault("sesgo_emoji", "🐂" if decision == "LONG" else "🐻" if decision == "SHORT" else "⚪")
    signal.setdefault("sesgo_texto", "COMPRA" if decision == "LONG" else "VENTA" if decision == "SHORT" else "SIN OPERAR")

    if decision not in ("LONG", "SHORT"):
        return signal

    try:
        entry = float(signal.get("entry_price"))
    except (TypeError, ValueError):
        logger.warning("⚠️ entry_price no numérico, no se puede calcular analisis_historico")
        return signal

    df15 = tfs.get("15m")
    if df15 is not None:
        historico = ofd.zone_reaction_history(df15, entry, decision)
        if historico:
            signal["analisis_historico"] = historico
        else:
            logger.info("ℹ️ Sin testeos históricos previos de esta zona en el 15m disponible")

    df1m = tfs.get("1m")
    if df1m is not None and len(df1m) >= 10:
        ventana = df1m.tail(10)
        precio_prom = float(ventana["close"].mean())
        signal["microestructura"] = {
            "volumen_absorcion_usd": round(float(ventana["volume"].sum()) * precio_prom, 0),
            "cvd_delta": round(float(ventana["delta"].sum()) * precio_prom, 0),
            "confluencias": _confluencias_texto(signal),
        }

    return signal


def _confluencias_texto(signal: dict) -> str:
    checklist = signal.get("confluence_checklist") or {}
    etiquetas = {
        "htf_structure_align": "Alineación HTF",
        "key_level_test": "Test de Nivel Clave",
        "micro_trigger_clear": "Trigger LTF Claro",
        "delta_confirmation": "Confirmación de Delta",
        "imbalance_stack_dir": "Imbalances Apilados",
        "bookmap_liq_support": "Soporte de Liquidez",
        "session_allowed": "Sesión Habilitada",
    }
    activos = [etiquetas[k] for k, v in checklist.items() if v and k in etiquetas]
    return ", ".join(activos) if activos else signal.get("setup_type", "N/A")


# ----------------------------------------------------------------------
# Telegram
# ----------------------------------------------------------------------

def _money(value) -> str:
    if value in (None, "N/A"):
        return "N/A"
    try:
        return f"{float(value):,.2f}"
    except (TypeError, ValueError):
        return str(value)


def _signed(value) -> str:
    if value in (None, "N/A"):
        return "N/A"
    try:
        v = float(value)
        return f"{'+' if v >= 0 else ''}{v:,.0f}"
    except (TypeError, ValueError):
        return str(value)


def _g(d: Optional[dict], key, default="N/A"):
    if not isinstance(d, dict):
        return default
    val = d.get(key)
    return default if val is None else val


def build_telegram_message(signal: dict) -> str:
    """Arma el mensaje con el formato pedido. Si falta un campo, muestra N/A -
    nunca lanza excepción por datos faltantes."""
    decision = signal.get("decision", "N/A")
    sesgo_emoji = signal.get("sesgo_emoji", "⚪")
    sesgo_texto = signal.get("sesgo_texto", "N/A")
    activo = signal.get("activo", "N/A")
    timeframe = signal.get("timeframe", "N/A")
    setup = signal.get("setup_type", "N/A")

    entry = signal.get("entry_price")
    sl = signal.get("stop_loss_price")
    tp_levels = signal.get("tp_levels") or []
    tp1 = tp_levels[0].get("price") if tp_levels and isinstance(tp_levels[0], dict) else None

    h = signal.get("analisis_historico") or {}
    m = signal.get("microestructura") or {}

    return f"""🎯 <b>NUEVA SEÑAL DE TRADING DETECTADA</b> (Institutional Liquidity Engine) 🚀

📈 <b>Activo:</b> {activo} ({timeframe})
🧠 <b>Estrategia:</b> {setup}
🧭 <b>Sesgo:</b> {sesgo_emoji} {sesgo_texto} ({decision})

💵 <b>Precio Entrada:</b> ${_money(entry)}
🛑 <b>Stop Loss:</b> ${_money(sl)}
🎯 <b>Take Profit Estimado:</b> ${_money(tp1)}

━━━━━━━━━━━━━━━━━━━━━━━━━
⏳ <b>ANÁLISIS DE REACCIÓN HISTÓRICA EN ESTA ZONA</b> 📊
<i>Backtest real sobre velas 15m previas en esta zona de precio</i>

🔄 <b>Testeos Previos:</b> {_g(h, "testeos_previos")} veces detectado en el histórico
⏱️ <b>Última Reacción:</b> hace {_g(h, "ultima_reaccion_velas")} velas (~{_g(h, "ultima_reaccion_horas")})
📈 <b>Tipo de Reacción:</b> {_g(h, "tipo_reaccion")}
🎯 <b>TP Histórico Promedio:</b> ${_money(h.get("tp_promedio"))} (+{_g(h, "tp_promedio_porcentaje")}% de rebote)
🏆 <b>TP Histórico Máximo (RECOMENDADO):</b> ${_money(h.get("tp_maximo"))} (+{_g(h, "tp_maximo_porcentaje")}% de recorrido máximo)
📈 <b>Efectividad de la Zona:</b> {_g(h, "efectividad_texto")}

━━━━━━━━━━━━━━━━━━━━━━━━━
📊 <b>MICROESTRUCTURA DE FLUJO DE ÓRDENES</b> ⚡
🌊 <b>Volumen de Absorción:</b> {_money(m.get("volumen_absorcion_usd"))} USD
📉 <b>CVD Delta:</b> {_signed(m.get("cvd_delta"))} USD
🔗 <b>Confluencias:</b> {_g(m, "confluencias")}
"""


def enviar_notificacion_signal(signal: dict) -> bool:
    """Envía la señal a Telegram si decision != NO_TRADE. Si falla, solo loguea
    el error y el script sigue (nunca frena el flujo principal)."""
    if signal.get("decision") in (None, "NO_TRADE", "CLOSE_PARTIAL", "CLOSE_ALL", "MOVE_SL_BE"):
        logger.info(f"ℹ️ decision={signal.get('decision')} - no se envía notificación a Telegram")
        return False

    try:
        mensaje = build_telegram_message(signal)
        ok = asyncio.run(enviar_telegram(mensaje))
        if ok:
            logger.info("📨 Señal enviada a Telegram")
        return ok
    except Exception as e:
        logger.error(f"❌ Error enviando a Telegram (el script continúa igual): {e}")
        return False


# ----------------------------------------------------------------------
# Firestore (panel de confluencias de la app de AI Studio)
# ----------------------------------------------------------------------

FIRESTORE_PROJECT_ID = "corded-braid-xk8sk"  # proyecto de la app de AI Studio
FIRESTORE_COLLECTION = "live_signals"

_firestore_client = None
_firestore_intentado = False


def _get_firestore_client():
    """Inicializa el cliente de Firestore una sola vez (lazy). Devuelve None si
    falta la librería o el login - nunca lanza excepción.

    Usa Application Default Credentials (login de gcloud como el propio usuario),
    NO una clave de servicio: el proyecto corded-braid-xk8sk está bajo una
    organización de Google (ai-builder-org) que bloquea la creación de claves de
    cuenta de servicio, así que no hay JSON de credencial posible ahí. Requiere
    haber corrido una vez:
        gcloud auth application-default login
        gcloud auth application-default set-quota-project corded-braid-xk8sk
    """
    global _firestore_client, _firestore_intentado
    if _firestore_client is not None or _firestore_intentado:
        return _firestore_client
    _firestore_intentado = True

    try:
        from google.cloud import firestore
    except ImportError:
        logger.warning("⚠️ google-cloud-firestore no instalado (pip install google-cloud-firestore) - no se guarda en Firestore")
        return None

    try:
        _firestore_client = firestore.Client(project=FIRESTORE_PROJECT_ID)
        logger.info(f"✅ Cliente de Firestore inicializado (proyecto: {FIRESTORE_PROJECT_ID})")
    except Exception as e:
        logger.error(
            f"❌ Error inicializando Firestore: {e}. "
            f"¿Corriste 'gcloud auth application-default login'?"
        )
        _firestore_client = None

    return _firestore_client


def save_signal_to_firestore(signal: dict, collection: str = FIRESTORE_COLLECTION) -> bool:
    """Guarda la señal en Firestore si decision != NO_TRADE, para que el panel
    de confluencias de la app de AI Studio la vea en tiempo real (onSnapshot).
    Si Firebase no está configurado o falla, solo loguea - nunca frena el script."""
    if signal.get("decision") in (None, "NO_TRADE", "CLOSE_PARTIAL", "CLOSE_ALL", "MOVE_SL_BE"):
        return False

    db = _get_firestore_client()
    if db is None:
        return False

    try:
        from google.cloud import firestore
        doc = {**signal, "created_at": datetime.now().isoformat(), "server_timestamp": firestore.SERVER_TIMESTAMP}
        db.collection(collection).add(doc)
        logger.info(f"🔥 Señal guardada en Firestore (colección '{collection}')")
        return True
    except Exception as e:
        logger.error(f"❌ Error guardando en Firestore (el script continúa igual): {e}")
        return False


# ----------------------------------------------------------------------
# Historial
# ----------------------------------------------------------------------

def save_signal_to_history(signal: dict, snapshot: dict) -> None:
    SIGNALS_HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        if SIGNALS_HISTORY_PATH.exists():
            history = json.loads(SIGNALS_HISTORY_PATH.read_text(encoding="utf-8"))
        else:
            history = {"signals": []}

        history["signals"].append({
            "timestamp": datetime.now().isoformat(),
            "signal": signal,
            "snapshot_htf": snapshot.get("snapshot", {}).get("htf", {}),
        })
        history["signals"] = history["signals"][-100:]

        SIGNALS_HISTORY_PATH.write_text(
            json.dumps(history, indent=2, ensure_ascii=False), encoding="utf-8"
        )
        logger.info(f"💾 Señal guardada en {SIGNALS_HISTORY_PATH}")
    except Exception as e:
        logger.error(f"❌ Error guardando historial: {e}")


def save_latest_signal(signal: dict) -> None:
    """Escribe la última señal para que la lea dashboard.html (fetch por polling).
    Se escribe siempre, incluso en NO_TRADE - el dashboard ya filtra esos client-side."""
    try:
        con_timestamp = {**signal, "timestamp": datetime.now().isoformat()}
        LATEST_SIGNAL_PATH.write_text(
            json.dumps(con_timestamp, indent=2, ensure_ascii=False), encoding="utf-8"
        )
    except Exception as e:
        logger.error(f"❌ Error guardando {LATEST_SIGNAL_PATH}: {e}")


# ----------------------------------------------------------------------
# Modo en vivo (WebSocket Binance vía src/snapshot_engine.py)
# ----------------------------------------------------------------------

def _htf_con_fallback_historico(engine, tfs_historico: dict) -> dict:
    """El motor en vivo necesita ~30 velas propias (30 min) para un volume
    profile con sentido estadístico. Mientras tanto, usa el volume profile
    histórico real como puente en vez de mandarle POC=0 a Gemini."""
    if engine.htf_listo():
        return engine.htf

    df_htf = tfs_historico.get("1h")
    if df_htf is None or df_htf.empty:
        return engine.htf

    vp = ofd.volume_profile(df_htf.tail(200))
    logger.info("ℹ️ HTF en vivo aún sin suficiente historia propia - usando volume profile histórico como puente")
    return {
        "poc": vp["poc"], "vah": vp["vah"], "val": vp["val"],
        "cvd_trend": ofd.cvd_trend(df_htf),
        "naked_pocs": ofd.naked_pocs(df_htf),
        "liq_clusters": ofd.liquidity_clusters(df_htf),
    }


async def run_ciclo_live(engine, equity_usdt: float) -> Optional[dict]:
    """Como run_ciclo(), pero el snapshot sale del motor en vivo (WebSocket)
    en vez de los CSV históricos. El backtest de analisis_historico sigue
    usando el 15m histórico (es un dato sobre el pasado, tiene sentido igual)."""
    payload = engine.get_live_snapshot(equity_usdt=equity_usdt)
    tfs_historico = ofd.load_symbol()
    payload["snapshot"]["htf"] = _htf_con_fallback_historico(engine, tfs_historico)

    htf = payload["snapshot"]["htf"]
    ltf_1m = payload["snapshot"]["ltf_1m"]
    logger.info(
        f"📊 Snapshot LIVE: POC={htf.get('poc')} VAH={htf.get('vah')} VAL={htf.get('val')} "
        f"CVD={htf.get('cvd_trend')} | Velas1m={len(ltf_1m)} | Trades/min={payload['snapshot']['tape_speed']}"
    )

    signal = ask_gemini_for_signal(payload)
    if not signal:
        print("\n❌ No se recibió señal. Revisá los logs arriba.")
        return None

    signal = enrich_signal_with_real_context(signal, tfs_historico)
    save_signal_to_history(signal, payload)
    save_latest_signal(signal)
    enviar_notificacion_signal(signal)
    save_signal_to_firestore(signal)

    print("\n✅ SEÑAL RECIBIDA:")
    print(json.dumps(signal, indent=2, ensure_ascii=False))
    return signal


async def main_live(equity_usdt: float, interval_min: int) -> None:
    from src.snapshot_engine import SnapshotEngine

    engine = SnapshotEngine("BTCUSDT")
    print("\n" + "=" * 60)
    print("🧠 CEREBRO ORDER FLOW (LIVE) - BTCUSDT")
    print("=" * 60 + "\n")

    await engine.start()
    logger.info("⏳ Esperando Snapshot Engine (conexión WS + primer vela 1m)...")
    await asyncio.sleep(75)  # tiempo real medido en pruebas para book listo + 1ra vela cerrada
    logger.info("✅ Snapshot Engine listo")

    ciclo = 0
    try:
        while True:
            ciclo += 1
            logger.info(f"--- CICLO {ciclo} (LIVE) ---")
            try:
                await run_ciclo_live(engine, equity_usdt)
            except Exception as e:
                logger.error(f"💥 Error en el ciclo {ciclo} (el loop sigue): {e}")
            logger.info(f"⏳ Esperando {interval_min} min hasta el próximo ciclo...")
            await asyncio.sleep(interval_min * 60)
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.warning("🛑 Señal de apagado recibida — cerrando Snapshot Engine...")
    finally:
        await engine.stop()


# ----------------------------------------------------------------------
# Modo event-driven (reacciona a patrones detectados, no a un timer)
# ----------------------------------------------------------------------

GEMINI_COOLDOWN_SEG = 2.0  # protección dura de rate-limit: free tier de Gemini = 60 req/min


def _procesar_evento_patron(payload: dict, equity_usdt: float) -> Optional[dict]:
    """Pipeline completo, síncrono a propósito: se llama vía run_in_executor
    para que el event loop del motor (WS) siga recibiendo trades/depth
    mientras Gemini responde (~15-30s), en vez de congelarse."""
    signal = ask_gemini_for_signal(payload)
    if not signal:
        return None

    if signal.get("decision") not in ("LONG", "SHORT") or signal.get("conviction", 0) < 6:
        logger.info(f"🔍 Patrón evaluado, sin confirmar: {signal.get('decision')} (conv {signal.get('conviction')})")
        return signal

    tfs_historico = ofd.load_symbol()
    signal = enrich_signal_with_real_context(signal, tfs_historico)
    save_signal_to_history(signal, payload)
    save_latest_signal(signal)
    enviar_notificacion_signal(signal)
    save_signal_to_firestore(signal)

    print("\n✅ SEÑAL CONFIRMADA (EVENT-DRIVEN):")
    print(json.dumps(signal, indent=2, ensure_ascii=False))
    return signal


async def main_event_driven(equity_usdt: float = 50000) -> None:
    from src.snapshot_engine import SnapshotEngine

    engine = SnapshotEngine("BTCUSDT")

    print("\n" + "=" * 60)
    print("🧠 CEREBRO ORDER FLOW (EVENT-DRIVEN) - BTCUSDT")
    print("=" * 60 + "\n")

    await engine.start()
    logger.info(
        "🎧 Escuchando patrones en vivo: el loop queda bloqueado en "
        "pattern_queue.get() sin sleep ni timeout hasta que el motor detecte "
        "absorción/iceberg/imbalances apilados. Cero polling. Ctrl+C para detener."
    )

    loop = asyncio.get_event_loop()
    ultimo_llamado = 0.0
    # ThreadPoolExecutor propio (no el default de run_in_executor): necesitamos
    # el concurrent.futures.Future crudo, sin envolver por asyncio, porque al
    # cancelar la task externa el wrapper de asyncio queda marcado "done" al
    # instante aunque el thread real siga corriendo - quedan desconectados y
    # ya no se puede esperar el resultado real a través de él. El Future crudo
    # no lo toca la cancelación de asyncio, así que sí sirve para esperarlo de
    # verdad en el apagado.
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    future_en_curso: Optional[concurrent.futures.Future] = None

    try:
        while True:
            evento = await engine.pattern_queue.get()  # bloquea indefinido, sin timeout

            ahora = time.time()
            if ahora - ultimo_llamado < GEMINI_COOLDOWN_SEG:
                # No es un heartbeat: solo actúa si llegan patrones más rápido
                # de lo que el free tier de Gemini (60 req/min) tolera.
                logger.info(f"⏭️ Patrón '{evento['tipo']}' descartado por rate-limit de Gemini")
                continue
            ultimo_llamado = ahora

            logger.info(f"⚡ Procesando patrón: {evento['tipo']} @ {evento['vela']['c']}...")
            future_en_curso = executor.submit(_procesar_evento_patron, evento["snapshot"], equity_usdt)
            try:
                await asyncio.wrap_future(future_en_curso)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"💥 Error procesando patrón (el loop sigue): {e}")

    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.warning("🛑 Señal de apagado recibida...")
        if future_en_curso is not None and not future_en_curso.done():
            logger.info("⏳ Esperando a que termine el patrón en curso (máx 60s)...")
            try:
                await loop.run_in_executor(None, future_en_curso.result, 60)
            except concurrent.futures.TimeoutError:
                logger.warning("⚠️ El patrón en curso no terminó en 60s - se cierra igual")
            except Exception:
                pass
    finally:
        logger.info("🛑 Cerrando Snapshot Engine...")
        executor.shutdown(wait=False)
        await engine.stop()


# ----------------------------------------------------------------------
# Modo determinista (decide() en Python, Gemini solo valida/narra)
# ----------------------------------------------------------------------
# Sin ejecución de órdenes reales en ningún caso - genera y notifica la señal
# exactamente igual que los otros modos (Telegram/Firestore/historial), no
# manda nada a ningún exchange.

def _procesar_evento_patron_determinista(payload: dict, equity_usdt: float, risk_manager, validator) -> Optional[dict]:
    """Síncrono a propósito (corre en el ThreadPoolExecutor dedicado de
    main_deterministic) - decide() es rapidísimo, pero la validación de
    Gemini y el I/O (Firestore/Telegram/disco) no, y no deben congelar el
    event loop del motor."""
    t0 = time.perf_counter()
    signal = decide(payload, equity_usdt, risk_manager)
    latencia_ms = (time.perf_counter() - t0) * 1000
    logger.info(f"📊 decide(): {signal['decision']} {signal['setup_type']} en {latencia_ms:.1f}ms")

    if signal["decision"] not in ("LONG", "SHORT"):
        logger.info(f"🔍 {signal['management_notes']}")
        return signal

    validacion = asyncio.run(validator.validate_and_explain(signal, payload["snapshot"]))
    if not validacion["approved"]:
        logger.warning(f"⚠️ Gemini vetó la decisión: {validacion['veto_reason']}")
        signal["decision"] = "NO_TRADE"
        signal["management_notes"] = f"VETADO POR GEMINI: {validacion['veto_reason']}"
        return signal

    signal["management_notes"] = f"{signal['management_notes']} | Gemini: {validacion['narrative']}"

    tfs_historico = ofd.load_symbol()
    signal = enrich_signal_with_real_context(signal, tfs_historico)
    save_signal_to_history(signal, payload)
    save_latest_signal(signal)
    enviar_notificacion_signal(signal)
    save_signal_to_firestore(signal)

    print("\n✅ SEÑAL DETERMINISTA CONFIRMADA:")
    print(json.dumps(signal, indent=2, ensure_ascii=False))
    return signal


def _colocar_ordenes_paper(paper_engine, signal: dict) -> Optional[dict]:
    """Coloca Entry LIMIT + OCO (TP1 LIMIT + SL STOP_MARKET) en el
    PaperEngine local. Se llama desde el event loop principal (no desde el
    executor thread) porque PaperEngine no es thread-safe a propósito - ver
    docstring de src/paper_engine.py. Devuelve la info que necesita
    _gestionar_bracket() para mover el SL a breakeven y colocar el TP2
    corredor una vez que TP1 se llena, o None si no se pudo colocar nada."""
    entry = signal.get("entry_price") or 0.0
    sl = signal.get("stop_loss_price") or 0.0
    tps = signal.get("tp_levels") or []
    if entry <= 0 or sl <= 0 or not tps:
        logger.warning("⚠️ Señal confirmada sin entry/SL/TP válidos - no se colocan órdenes paper")
        return None

    qty = round((signal.get("position_size_usdt", 0) / entry) / c.STEP_SIZE) * c.STEP_SIZE
    if qty <= 0:
        logger.warning("⚠️ Tamaño de posición calculado en 0 - no se colocan órdenes paper")
        return None

    direccion = signal["decision"]
    side = "BUY" if direccion == "LONG" else "SELL"
    close_side = "SELL" if side == "BUY" else "BUY"
    tp1_pct = tps[0].get("size_pct", 50) / 100
    tp1_qty = round((qty * tp1_pct) / c.STEP_SIZE) * c.STEP_SIZE or qty
    qty_restante = round((qty - tp1_qty) / c.STEP_SIZE) * c.STEP_SIZE

    try:
        entry_order = paper_engine.new_order(symbol=c.SYMBOL, side=side, type="LIMIT", quantity=qty, price=entry)
        # TP1 y SL comparten oco_id: al llenarse una, PaperEngine cancela la
        # otra - si no, la que quedó "NEW" puede disparar mucho después
        # contra la posición de una señal totalmente distinta (bug real que
        # produjo PnL de +-75,000 USDT en paper trading).
        bracket_id = uuid.uuid4().hex[:12]
        tp1_order = paper_engine.new_order(
            symbol=c.SYMBOL, side=close_side, type="LIMIT", quantity=tp1_qty, price=tps[0]["price"],
            reduceOnly=True, oco_id=bracket_id,
        )
        sl_order = paper_engine.new_order(
            symbol=c.SYMBOL, side=close_side, type="STOP_MARKET", quantity=qty, stopPrice=sl,
            reduceOnly=True, oco_id=bracket_id,
        )
        logger.info(
            f"📈 ORDER: {direccion} {qty} @ {entry} SL {sl} TP1 {tps[0]['price']}"
            + (f" TP2 {tps[1]['price']}" if len(tps) > 1 else "")
        )
        logger.info(
            f"📝 PAPER ÓRDENES: Entry={entry_order['orderId']} TP1={tp1_order['orderId']} SL={sl_order['orderId']}"
        )

        if len(tps) < 2 or qty_restante <= 0:
            return None
        return {
            "direccion": direccion, "entry": entry, "tp1_id": tp1_order["orderId"],
            "sl_id": sl_order["orderId"], "tp2_price": tps[1]["price"], "qty_restante": qty_restante,
            "close_side": close_side,
        }
    except Exception as e:
        logger.error(f"❌ Error colocando órdenes paper (la señal ya se notificó igual): {e}")
        return None


async def _gestionar_bracket(paper_engine, bracket: dict) -> None:
    """Gestión híbrida post-entry: espera a que TP1 o el SL inicial se
    llenen. Si el SL dispara primero, la posición quedó cerrada del todo -
    no hay nada más que hacer. Si TP1 dispara primero (su hermano SL ya se
    auto-canceló vía oco_id), mueve el resto de la posición a breakeven +
    BREAKEVEN_BUFFER_PIPS y coloca el TP2 (corredor) - ambos emparejados con
    un oco_id nuevo para que uno cancele al otro al llenarse."""
    tp1_id, sl_id = bracket["tp1_id"], bracket["sl_id"]
    direccion, entry = bracket["direccion"], bracket["entry"]

    while True:
        await asyncio.sleep(1.0)
        tp1 = paper_engine.get_order(symbol=c.SYMBOL, orderId=tp1_id)
        if tp1.get("status") == "FILLED":
            break
        sl = paper_engine.get_order(symbol=c.SYMBOL, orderId=sl_id)
        if sl.get("status") == "FILLED":
            logger.info("🛑 SL HIT antes de TP1 - posición cerrada, bracket terminado")
            return
        if sl.get("status") == "CANCELED":
            # No debería pasar en el ciclo de vida normal (el SL solo se
            # cancela cuando TP1 se llena, y ese caso ya rompió el loop
            # arriba) - defensivo, nada más que gestionar acá.
            return

    be_sl = entry + c.BREAKEVEN_BUFFER_PIPS if direccion == "LONG" else entry - c.BREAKEVEN_BUFFER_PIPS
    bracket_id2 = uuid.uuid4().hex[:12]
    try:
        nuevo_sl = paper_engine.new_order(
            symbol=c.SYMBOL, side=bracket["close_side"], type="STOP_MARKET", quantity=bracket["qty_restante"],
            stopPrice=round(be_sl, 2), reduceOnly=True, oco_id=bracket_id2,
        )
        nuevo_tp2 = paper_engine.new_order(
            symbol=c.SYMBOL, side=bracket["close_side"], type="LIMIT", quantity=bracket["qty_restante"],
            price=round(bracket["tp2_price"], 2), reduceOnly=True, oco_id=bracket_id2,
        )
        logger.info(
            f"🎯 TP1 HIT: 50% cerrado, SL -> BE+{c.BREAKEVEN_BUFFER_PIPS:.0f} @ {be_sl:.2f} ({nuevo_sl['orderId']}) "
            f"| TP2 corredor @ {bracket['tp2_price']:.2f} ({nuevo_tp2['orderId']})"
        )
    except Exception as e:
        logger.error(f"❌ Error re-colocando SL/TP2 tras TP1 (posición parcial queda sin proteger): {e}")


async def main_deterministic(equity_usdt: float = 50000, paper: bool = False) -> None:
    from src.snapshot_engine import SnapshotEngine
    from src.risk_manager import RiskManager
    from src.gemini_validator import GeminiValidator
    from src.discord_liquidity_zones import DiscordLiquidityZonesClient
    from src.zone_performance_db import sync_from_firestore

    n_zonas = sync_from_firestore()  # una sola vez al arrancar - decide() solo lee el cache en memoria, nunca esto
    logger.info(f"📈 zone_stats sincronizado desde Firestore: {n_zonas} tipos de zona" if n_zonas else "📈 zone_stats: sin datos en Firestore, decide() sigue sin el filtro histórico")

    discord_zones = DiscordLiquidityZonesClient()
    engine = SnapshotEngine("BTCUSDT", discord_zones_client=discord_zones)
    risk_manager = RiskManager(_get_firestore_client(), capital_inicial=equity_usdt)
    validator = GeminiValidator(getattr(settings, "GEMINI_API_KEY", "") or "")

    paper_engine = None
    if paper:
        from src.paper_engine import PaperEngine
        paper_engine = PaperEngine(engine, risk_manager)

    print("\n" + "=" * 60)
    print("🧠 CEREBRO ORDER FLOW (DETERMINISTA) - BTCUSDT")
    print("=" * 60)
    print("decide() en Python puro (<50ms, sin Gemini) -> Gemini valida/narra")
    print("(timeout 800ms) -> notifica" + (" -> paper trading local" if paper else "") + ".")
    print("Sin ejecución de órdenes reales en ningún exchange." + (" Paper trading: 100% local." if paper else ""))
    print("=" * 60 + "\n")

    await discord_zones.start()  # opcional - si falla, el resto sigue igual
    await engine.start()
    if paper_engine is not None:
        await paper_engine.start()
    logger.info("🎧 Escuchando patrones en vivo (determinista). Ctrl+C para detener.")

    loop = asyncio.get_event_loop()
    ultimo_llamado = 0.0
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    future_en_curso: Optional[concurrent.futures.Future] = None
    bracket_tasks: list[asyncio.Task] = []

    try:
        while True:
            evento = await engine.pattern_queue.get()  # bloquea indefinido, sin timeout

            if not engine.htf_listo():
                logger.info(f"⏳ Patrón '{evento['tipo']}' ignorado - HTF no listo todavía ({len(engine.klines_1m)}/{c.HTF_READY_MINUTES} min)")
                continue

            equity_state = await risk_manager.get_equity_state()
            if equity_state.limite_diario_superado:
                logger.warning(
                    f"🛑 Límite de pérdida diaria superado ({equity_state.perdida_diaria_pct:.2%} >= "
                    f"{c.DAILY_LOSS_LIMIT:.2%}) - sin operar hasta mañana"
                )
                continue

            ahora = time.time()
            if ahora - ultimo_llamado < GEMINI_COOLDOWN_SEG:
                logger.info(f"⏭️ Patrón '{evento['tipo']}' descartado por rate-limit de Gemini")
                continue
            ultimo_llamado = ahora

            logger.info(f"⚡ Procesando patrón: {evento['tipo']} @ {evento['vela']['c']}...")
            future_en_curso = executor.submit(
                _procesar_evento_patron_determinista, evento["snapshot"], equity_state.equity, risk_manager, validator,
            )
            try:
                signal = await asyncio.wrap_future(future_en_curso)
                if paper_engine is not None and signal and signal.get("decision") in ("LONG", "SHORT"):
                    bracket = _colocar_ordenes_paper(paper_engine, signal)
                    if bracket is not None:
                        bracket_tasks.append(asyncio.create_task(_gestionar_bracket(paper_engine, bracket)))
                    bracket_tasks = [t for t in bracket_tasks if not t.done()]
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"💥 Error procesando patrón (el loop sigue): {e}")

    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.warning("🛑 Señal de apagado recibida...")
        if future_en_curso is not None and not future_en_curso.done():
            logger.info("⏳ Esperando a que termine el patrón en curso (máx 60s)...")
            try:
                await loop.run_in_executor(None, future_en_curso.result, 60)
            except concurrent.futures.TimeoutError:
                logger.warning("⚠️ El patrón en curso no terminó en 60s - se cierra igual")
            except Exception:
                pass
    finally:
        logger.info("🛑 Cerrando Snapshot Engine...")
        executor.shutdown(wait=False)
        for t in bracket_tasks:
            if not t.done():
                t.cancel()
        if bracket_tasks:
            await asyncio.gather(*bracket_tasks, return_exceptions=True)
        if paper_engine is not None:
            await paper_engine.stop()
        await engine.stop()
        await discord_zones.stop()


# ----------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------

def run_ciclo(equity_usdt: float) -> Optional[dict]:
    """Ejecuta un ciclo completo: carga datos, arma snapshot, consulta a Gemini,
    enriquece con contexto real, guarda historial y notifica si corresponde."""
    tfs = ofd.load_symbol()
    payload = build_snapshot(tfs, equity_usdt=equity_usdt)
    signal = ask_gemini_for_signal(payload)

    if not signal:
        print("\n❌ No se recibió señal. Revisá los logs arriba.")
        print("🔧 Verificá: GEMINI_API_KEY en config/settings.py (debe ser una clave de AI Studio válida).\n")
        return None

    signal = enrich_signal_with_real_context(signal, tfs)
    save_signal_to_history(signal, payload)
    save_latest_signal(signal)
    enviar_notificacion_signal(signal)
    save_signal_to_firestore(signal)

    print("\n✅ SEÑAL RECIBIDA:")
    print(json.dumps(signal, indent=2, ensure_ascii=False))
    return signal


def main():
    parser = argparse.ArgumentParser(description="Cerebro Order Flow - genera una señal con datos reales de BTCUSDT")
    parser.add_argument("--equity", type=float, default=50000, help="Equity simulado en USDT")
    parser.add_argument("--loop", action="store_true", help="Correr en bucle continuo en vez de una sola vez")
    parser.add_argument("--interval", type=int, default=5, help="Minutos entre ciclos en modo --loop/--live (default: 5)")
    parser.add_argument("--live", action="store_true", help="Usar WebSocket de Binance en vivo en vez de los CSV históricos (implica loop continuo)")
    parser.add_argument("--event-driven", action="store_true", help="Reaccionar a patrones detectados (absorción/iceberg/imbalances apilados) en vez de a un timer. Implica --live.")
    parser.add_argument("--deterministic", action="store_true", help="Decisión 100%% Python (sin Gemini, <50ms) + Gemini como validador/narrador async (timeout 800ms). Implica --live. Sin ejecución de órdenes reales.")
    parser.add_argument("--paper", action="store_true", help="Con --deterministic: simula fills localmente (paper trading, 100%% local, sin exchange real) y actualiza el equity en Firestore.")
    args = parser.parse_args()

    if args.deterministic:
        try:
            asyncio.run(main_deterministic(args.equity, paper=args.paper))
        except KeyboardInterrupt:
            pass
        return

    if args.event_driven:
        try:
            asyncio.run(main_event_driven(args.equity))
        except KeyboardInterrupt:
            pass
        return

    if args.live:
        try:
            asyncio.run(main_live(args.equity, args.interval))
        except KeyboardInterrupt:
            pass
        return

    print("\n" + "=" * 60)
    print("🧠 CEREBRO ORDER FLOW - BTCUSDT")
    print("=" * 60 + "\n")

    if not args.loop:
        run_ciclo(args.equity)
        return

    logger.info(
        f"🔄 Modo loop: un ciclo cada {args.interval} min. Ctrl+C para detener. "
        f"OJO: los CSV de order flow son una foto histórica fija (ver orderflow_data.py) "
        f"- hasta que se conecte un feed en vivo, cada ciclo va a consultar prácticamente "
        f"el mismo snapshot."
    )
    ciclo = 0
    try:
        while True:
            ciclo += 1
            logger.info(f"--- CICLO {ciclo} ---")
            try:
                run_ciclo(args.equity)
            except Exception as e:
                logger.error(f"💥 Error en el ciclo {ciclo} (el loop sigue): {e}")
            logger.info(f"⏳ Esperando {args.interval} min hasta el próximo ciclo...")
            time.sleep(args.interval * 60)
    except KeyboardInterrupt:
        logger.info("🛑 Loop detenido por el usuario")


if __name__ == "__main__":
    main()
