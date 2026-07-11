"""
Prueba el formato del mensaje de Telegram (armado + envío) sin esperar una señal
real de Gemini. Usa una señal de ejemplo con todos los campos.

Uso:
    python test_formato.py            # arma y envía el mensaje a Telegram
    python test_formato.py --solo-armar   # solo imprime el mensaje, no lo envía
"""
import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass

from src.order_flow_signal import build_telegram_message
from src.notifier import enviar_telegram

SIGNAL_EJEMPLO = {
    "decision": "LONG",
    "conviction": 8,
    "setup_type": "CONFLUENCIA_ESTRUCTURAS",
    "sesgo_emoji": "🐂",
    "sesgo_texto": "COMPRA",
    "activo": "BTCUSDT",
    "timeframe": "15m",
    "entry_price": 68420.0,
    "stop_loss_price": 68180.0,
    "position_size_usdt": 3500.0,
    "tp_levels": [
        {"price": 69140.0, "size_pct": 50, "logic": "TP Principal"},
        {"price": 69580.0, "size_pct": 50, "logic": "TP Extendido"},
    ],
    "htf_context_1line": "Bias LONG: POC Migra UP, CVD Acumula, Precio Testea VAL.",
    "ltf_trigger_detail": "ABSORPTION @ VAL. Sell Delta absorbido -> Reclaim -> Flip alcista.",
    "confluence_checklist": {
        "htf_structure_align": True,
        "key_level_test": True,
        "micro_trigger_clear": True,
        "delta_confirmation": True,
        "imbalance_stack_dir": False,
        "bookmap_liq_support": False,
        "session_allowed": True,
    },
    "risk_metrics": {"r_multiple_tp1": 3.0, "risk_pct_account": 0.5, "time_stop_min": 30},
    "execution_orders": [],
    "management_notes": "Mover SL a BE al tocar TP1.",
    # analisis_historico y microestructura: en producción los calcula
    # enrich_signal_with_real_context() con datos reales, acá van fijos de ejemplo.
    "analisis_historico": {
        "testeos_previos": 3,
        "ultima_reaccion_velas": 28,
        "ultima_reaccion_horas": "7.0h",
        "tipo_reaccion": "Rechazo de Mecha Violento",
        "tp_promedio": 69220.0,
        "tp_promedio_porcentaje": 1.17,
        "tp_maximo": 69580.0,
        "tp_maximo_porcentaje": 1.70,
        "efectividad_zona": 75,
        "efectividad_texto": "75% Win Rate histórico en este nivel (3 testeos)",
    },
    "microestructura": {
        "volumen_absorcion_usd": 1600000,
        "cvd_delta": 14820,
        "confluencias": "Alineación HTF, Test de Nivel Clave, Trigger LTF Claro, Confirmación de Delta",
    },
}


def main():
    parser = argparse.ArgumentParser(description="Prueba el formato del mensaje de Telegram")
    parser.add_argument("--solo-armar", action="store_true", help="No enviar, solo imprimir el mensaje armado")
    args = parser.parse_args()

    mensaje = build_telegram_message(SIGNAL_EJEMPLO)

    print("\n" + "=" * 60)
    print("MENSAJE ARMADO:")
    print("=" * 60)
    print(mensaje)
    print("=" * 60 + "\n")

    if args.solo_armar:
        print("ℹ️ --solo-armar: no se envió nada a Telegram.")
        return

    print("📤 Enviando a Telegram...")
    ok = asyncio.run(enviar_telegram(mensaje))
    if ok:
        print("\n✅ Revisá tu Telegram, debería haber llegado el mensaje con este formato.")
    else:
        print("\n❌ No se pudo enviar. Revisá TELEGRAM_BOT_TOKEN y TELEGRAM_CHAT_ID en config/settings.py.")


if __name__ == "__main__":
    main()
