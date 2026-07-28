"""
╔══════════════════════════════════════════════════════════╗
║  NOTIFIER - Sistema de notificaciones                   ║
║  Envía alertas por Telegram                             ║
╚══════════════════════════════════════════════════════════╝
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass

import asyncio
import aiohttp
from datetime import datetime

from config.settings import (
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID,
)

async def enviar_telegram(mensaje: str):
    """Envía un mensaje a Telegram."""
    if "PON_TU" in TELEGRAM_BOT_TOKEN:
        print("❌ ERROR: TELEGRAM_BOT_TOKEN no configurado en settings.py")
        print("   Edita config/settings.py y pon tu token real")
        return False
    
    if "PON_TU" in TELEGRAM_CHAT_ID:
        print("❌ ERROR: TELEGRAM_CHAT_ID no configurado en settings.py")
        print("   Edita config/settings.py y pon tu chat ID real")
        return False
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": mensaje,
        "parse_mode": "HTML"
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload) as resp:
                data = await resp.json()
                if data.get("ok"):
                    print(f"✅ Telegram: Mensaje enviado correctamente")
                    return True
                else:
                    error_desc = data.get('description', 'Error desconocido')
                    print(f"❌ Telegram Error: {error_desc}")
                    
                    # Errores comunes y soluciones
                    if "chat not found" in error_desc.lower():
                        print("\n💡 SOLUCIÓN: Abre Telegram y envíale un mensaje a tu bot primero.")
                        print("   El bot no puede iniciar conversación contigo.")
                    elif "unauthorized" in error_desc.lower():
                        print("\n💡 SOLUCIÓN: El token es incorrecto. Revísalo en @BotFather → /mybots")
                    
                    return False
    except Exception as e:
        print(f"❌ Error de conexión: {e}")
        return False


async def main():
    print("╔══════════════════════════════════════╗")
    print("║  PRUEBA DE NOTIFICACIONES           ║")
    print("╚══════════════════════════════════════╝")
    
    print(f"\n📱 Probando Telegram...")
    print(f"   Token: {TELEGRAM_BOT_TOKEN[:20]}...")
    print(f"   Chat ID: {TELEGRAM_CHAT_ID}")
    
    resultado = await enviar_telegram(
        f"✅ <b>Prueba exitosa</b>\n\n"
        f"Sistema de notificaciones operativo\n\n"
        f"⏰ {datetime.now().strftime('%H:%M:%S')}"
    )
    
    if resultado:
        print("\n🎉 ¡LISTO! Revisa tu Telegram.")
        print("   Deberías tener un mensaje nuevo de tu bot.")
    else:
        print("\n❌ ALGO FALLÓ.")
        print("\n📋 Checklist:")
        print("   1. ¿Pusiste el token CORRECTO en settings.py?")
        print("   2. ¿Pusiste tu chat ID CORRECTO en settings.py?")
        print("   3. ¿Le enviaste al menos 1 mensaje a tu bot en Telegram?")
        print("   4. ¿El bot y el chat ID son del MISMO bot?")


if __name__ == "__main__":
    asyncio.run(main())
