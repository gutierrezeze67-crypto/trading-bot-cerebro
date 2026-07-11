"""
Genera señales de trading con el "cerebro" de Order Flow (config/systemInstruction.txt)
usando datos reales de BTCUSDT (ver src/orderflow_data.py para qué es real vs aproximado).

Uso:
    python -m src.order_flow_signal
    python -m src.order_flow_signal --equity 25000
"""
import argparse
import asyncio
import json
import logging
import os
import sys
import time
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
from src import orderflow_data as ofd
from src.notifier import enviar_telegram

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
        max_output_tokens=8192,
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

FIREBASE_CRED_PATH = BASE_DIR / "config" / "firebase-service-account.json"
FIRESTORE_COLLECTION = "live_signals"

_firestore_client = None
_firestore_intentado = False


def _get_firestore_client():
    """Inicializa el cliente de Firestore una sola vez (lazy). Devuelve None si
    falta la librería o la credencial - nunca lanza excepción."""
    global _firestore_client, _firestore_intentado
    if _firestore_client is not None or _firestore_intentado:
        return _firestore_client
    _firestore_intentado = True

    try:
        import firebase_admin
        from firebase_admin import credentials, firestore
    except ImportError:
        logger.warning("⚠️ firebase-admin no instalado (pip install firebase-admin) - no se guarda en Firestore")
        return None

    if not FIREBASE_CRED_PATH.exists():
        logger.warning(
            f"⚠️ No se encontró {FIREBASE_CRED_PATH} - no se guarda en Firestore. "
            f"Descargala desde Firebase Console > Configuración del proyecto > Cuentas de servicio."
        )
        return None

    try:
        if not firebase_admin._apps:
            cred = credentials.Certificate(str(FIREBASE_CRED_PATH))
            firebase_admin.initialize_app(cred)
        _firestore_client = firestore.client()
        logger.info("✅ Cliente de Firestore inicializado")
    except Exception as e:
        logger.error(f"❌ Error inicializando Firebase: {e}")
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
        from firebase_admin import firestore
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
    parser.add_argument("--interval", type=int, default=5, help="Minutos entre ciclos en modo --loop (default: 5)")
    args = parser.parse_args()

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
