import os
import json
import requests
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from pathlib import Path

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# -----------------------------------------------------------
# CONFIG GEMINI
# -----------------------------------------------------------
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
if not GEMINI_API_KEY:
    logger.warning("⚠️ GEMINI_API_KEY no está en variables de entorno.")
    GEMINI_API_KEY = input("🔑 Ingresá tu API Key de Gemini: ")

GEMINI_URL = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={GEMINI_API_KEY}"

# -----------------------------------------------------------
# SYSTEM INSTRUCTION (cargada desde archivo)
# -----------------------------------------------------------
BASE_DIR = Path(__file__).parent
SYSTEM_INSTRUCTION_PATH = BASE_DIR / "config" / "systemInstruction.txt"

def load_system_instruction() -> str:
    """Carga la System Instruction desde archivo."""
    try:
        with open(SYSTEM_INSTRUCTION_PATH, "r", encoding="utf-8") as f:
            content = f.read()
            logger.info(f"✅ System Instruction cargada ({len(content)} caracteres)")
            return content
    except FileNotFoundError:
        logger.error(f"❌ No se encontró systemInstruction.txt en {SYSTEM_INSTRUCTION_PATH}")
        return "Eres un trader institucional especializado en Order Flow. Responde solo JSON válido."
    except Exception as e:
        logger.error(f"❌ Error cargando System Instruction: {e}")
        return "Eres un trader institucional. Responde solo JSON válido."

SYSTEM_INSTRUCTION = load_system_instruction()

# -----------------------------------------------------------
# FUNCIÓN PRINCIPAL: ENVÍA SNAPSHOT A GEMINI
# -----------------------------------------------------------
def ask_gemini_for_signal(
    snapshot_dict: Dict,
    equity_usdt: float = 50000,
    open_positions: Optional[List[Dict]] = None,
    timeout: int = 15
) -> Optional[Dict]:
    """Envía snapshot a Gemini y retorna la señal."""
    if open_positions is None:
        open_positions = []

    payload = {
        "contents": [{
            "role": "user",
            "parts": [{"text": json.dumps({
                "snapshot": snapshot_dict,
                "equity_usdt": equity_usdt,
                "open_positions": open_positions
            }, ensure_ascii=False)}]
        }],
        "systemInstruction": {
            "parts": [{"text": SYSTEM_INSTRUCTION}]
        },
        "generationConfig": {
            "temperature": 0.15,
            "topP": 0.9,
            "topK": 20,
            "maxOutputTokens": 8192,
            "responseMimeType": "application/json"
        }
    }

    headers = {"Content-Type": "application/json"}
    raw_text = ""

    try:
        logger.info(f"📤 Enviando snapshot a Gemini...")
        response = requests.post(GEMINI_URL, headers=headers, json=payload, timeout=timeout)
        response.raise_for_status()
        data = response.json()

        raw_text = data["candidates"][0]["content"]["parts"][0]["text"].strip()
        
        # Limpiar markdown
        if raw_text.startswith("```json"):
            raw_text = raw_text[7:]
        if raw_text.endswith("```"):
            raw_text = raw_text[:-3]
        raw_text = raw_text.strip()

        signal = json.loads(raw_text)
        
        required_fields = ["decision", "entry_price", "stop_loss_price", "tp_levels"]
        if all(field in signal for field in required_fields):
            logger.info(f"✅ Señal: {signal.get('decision')} | Conviction: {signal.get('conviction', 0)}")
            return signal
        else:
            logger.error(f"❌ Señal incompleta")
            return None

    except requests.exceptions.Timeout:
        logger.error(f"❌ Timeout")
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Error HTTP: {e}")
    except json.JSONDecodeError as e:
        logger.error(f"❌ Error JSON: {e}")
        logger.debug(f"Raw: {raw_text[:200]}")
    except Exception as e:
        logger.error(f"❌ Error inesperado: {e}")

    return None

# -----------------------------------------------------------
# GUARDAR SEÑAL EN HISTORIAL
# -----------------------------------------------------------
SIGNALS_HISTORY_PATH = BASE_DIR / "data" / "signals_history.json"
SIGNALS_HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)

def save_signal_to_history(signal: Dict, snapshot: Dict) -> None:
    """Guarda la señal en historial."""
    try:
        if SIGNALS_HISTORY_PATH.exists():
            with open(SIGNALS_HISTORY_PATH, "r", encoding="utf-8") as f:
                history = json.load(f)
        else:
            history = {"signals": []}

        entry = {
            "timestamp": datetime.now().isoformat(),
            "signal": signal,
            "snapshot": {
                "htf": snapshot.get("htf", {}),
            }
        }
        history["signals"].append(entry)

        if len(history["signals"]) > 100:
            history["signals"] = history["signals"][-100:]

        with open(SIGNALS_HISTORY_PATH, "w", encoding="utf-8") as f:
            json.dump(history, f, indent=2, ensure_ascii=False)

        logger.info(f"💾 Señal guardada en historial")

    except Exception as e:
        logger.error(f"❌ Error guardando historial: {e}")

# -----------------------------------------------------------
# PRUEBA RÁPIDA
# -----------------------------------------------------------
if __name__ == "__main__":
    print("🧪 EJECUTANDO PRUEBA...")
    
    test_snapshot = {
        "htf": {"poc": 64500, "vah": 64800, "val": 64200, "cvd_trend": "UP"},
        "ltf_1m": [
            {"t": "14:30:00", "c": 64205, "delta": -4200, "vol": 850, 
             "imb": [{"p": 64202, "r": 4.2, "s": "bid"}], "abs": True, "iceberg": 64200}
        ],
        "tape_speed": 120,
        "bookmap": {"bid_iceberg": 64200, "ask_walls": [[64500, 2000]]}
    }
    
    print("📤 Enviando snapshot de prueba...")
    signal = ask_gemini_for_signal(test_snapshot, equity_usdt=50000)
    
    if signal:
        print("\n✅ SEÑAL RECIBIDA:")
        print(json.dumps(signal, indent=2, ensure_ascii=False))
    else:
        print("\n❌ No se recibió señal")