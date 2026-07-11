"""
╔══════════════════════════════════════════════════════════╗
║  DISCORD ZONES READER - Lector de imagenes de Trading   ║
║  Different para analisis de zonas de liquidez           ║
╚══════════════════════════════════════════════════════════╝

Flujo:
1. Bot de Discord escucha el canal de Trading Different
2. Detecta imagenes subidas (screenshots de zonas de liquidez)
3. Descarga la imagen en alta calidad
4. La envia a Gemini 2.5 Flash para analisis
5. Extrae zonas de liquidez con precios (ej: 65.500 - 65.600)
6. Guarda el analisis en data/analisis_zonas/
7. Notifica por Telegram cuando detecta zonas relevantes
"""

import asyncio
import json
import os
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import discord
from discord.ext import commands
import google.generativeai as genai
import aiohttp

from config.settings import (
    DISCORD_BOT_TOKEN,
    DISCORD_ZONAS_CHANNEL_ID,
    GEMINI_API_KEY,
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID,
    SYMBOLS,
)

# ══════════════════════════════════════════════════════════
# CONFIGURACION INICIAL
# ══════════════════════════════════════════════════════════

genai.configure(api_key=GEMINI_API_KEY)

# Modelo sin limites de rate (pago)
model = genai.GenerativeModel('gemini-2.5-flash')

DATA_DIR = Path(__file__).parent.parent / "data"
IMAGES_DIR = DATA_DIR / "imagenes_zonas"
ANALISIS_DIR = DATA_DIR / "analisis_zonas"

IMAGES_DIR.mkdir(parents=True, exist_ok=True)
ANALISIS_DIR.mkdir(parents=True, exist_ok=True)

# ══════════════════════════════════════════════════════════
# FUNCIONES
# ══════════════════════════════════════════════════════════

async def enviar_telegram(mensaje: str):
    """Envia notificacion por Telegram."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": mensaje,
        "parse_mode": "HTML"
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as resp:
            return await resp.json()


async def descargar_imagen(url: str, filename: str) -> Path:
    """Descarga una imagen desde Discord CDN."""
    filepath = IMAGES_DIR / filename
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            if resp.status == 200:
                data = await resp.read()
                with open(filepath, 'wb') as f:
                    f.write(data)
                print(f"✅ Imagen descargada: {filename}")
                return filepath
            else:
                print(f"❌ Error descargando imagen: {resp.status}")
                return None


async def analizar_imagen_gemini(image_path: Path) -> dict:
    """Envia la imagen a Gemini para extraer zonas de liquidez."""
    
    with open(image_path, 'rb') as f:
        image_data = f.read()
    
    image_part = {
        "mime_type": "image/png",
        "data": image_data
    }
    
    prompt = """
Eres un analista de zonas de liquidez profesional. Analiza esta imagen de Trading Different.

INSTRUCCIONES:
1. Busca TODOS los rectangulos dibujados en la imagen (zonas de liquidez)
2. Para cada rectangulo, extrae EXACTAMENTE:
   - Precio superior (borde de arriba del rectangulo)
   - Precio inferior (borde de abajo del rectangulo)
3. Identifica el activo/simbolo si aparece en la imagen
4. Determina si cada zona es de COMPRA (buy side liquidity) o VENTA (sell side liquidity)

FORMATO DE RESPUESTA (JSON):
{
  "activo_detectado": "XAUUSDT",
  "zonas_liquidez": [
    {
      "tipo": "BUY_SIDE",
      "precio_superior": 2650.50,
      "precio_inferior": 2645.00,
      "rango": "2645.00 - 2650.50",
      "confianza": "ALTA"
    }
  ],
  "resumen": "Se detecto 1 zona de compra en 2645-2650.50"
}

Si no hay rectangulos, devuelve:
{
  "zonas_liquidez": [],
  "resumen": "No se detectaron zonas de liquidez"
}

Se EXACTO con los precios. Cada decimal importa.
"""
    
    try:
        response = model.generate_content([prompt, image_part])
        texto_respuesta = response.text
        
        if "```json" in texto_respuesta:
            texto_respuesta = texto_respuesta.split("```json")[1].split("```")[0].strip()
        elif "```" in texto_respuesta:
            texto_respuesta = texto_respuesta.split("```")[1].split("```")[0].strip()
        
        analisis = json.loads(texto_respuesta)
        analisis["imagen_original"] = str(image_path.name)
        analisis["timestamp_procesamiento"] = datetime.now().isoformat()
        
        print(f"✅ Analisis: {len(analisis.get('zonas_liquidez', []))} zonas detectadas")
        return analisis
        
    except json.JSONDecodeError:
        print(f"⚠️ Gemini no devolvio JSON valido. Respuesta: {response.text[:300]}")
        return {
            "error": "JSON no valido",
            "respuesta_cruda": response.text,
            "imagen_original": str(image_path.name)
        }
    except Exception as e:
        print(f"❌ Error en analisis Gemini: {e}")
        return {
            "error": str(e),
            "imagen_original": str(image_path.name)
        }


def guardar_analisis(analisis: dict, filename: str):
    """Guarda el analisis en JSON."""
    filepath = ANALISIS_DIR / filename
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(analisis, f, indent=2, ensure_ascii=False)
    print(f"💾 Analisis guardado: {filename}")


def es_activo_relevante(analisis: dict) -> bool:
    """Verifica si el activo detectado esta en nuestra lista."""
    activo = analisis.get("activo_detectado", "").upper().replace(" ", "")
    
    mapeo_activos = {
        "XAUUSD": "XAUUSDT",
        "XAU/USD": "XAUUSDT",
        "GOLD": "XAUUSDT",
        "ORO": "XAUUSDT",
        "XAGUSD": "XAGUSDT",
        "XAG/USD": "XAGUSDT",
        "SILVER": "XAGUSDT",
        "PLATA": "XAGUSDT",
        "WTI": "USOIL",
        "OIL": "USOIL",
        "CRUDE": "USOIL",
        "US30": "US30",
        "DOW": "US30",
        "DOWJONES": "US30",
        "SPX": "SP500",
        "SP500": "SP500",
        "S&P500": "SP500",
        "NDX": "NAS100",
        "NASDAQ": "NAS100",
        "NAS100": "NAS100",
        "BTC": "BTCUSDT",
        "BTCUSD": "BTCUSDT",
        "BITCOIN": "BTCUSDT",
    }
    
    activo_normalizado = mapeo_activos.get(activo, activo)
    return activo_normalizado in [s.upper() for s in SYMBOLS]


# ══════════════════════════════════════════════════════════
# BOT DE DISCORD
# ══════════════════════════════════════════════════════════

intents = discord.Intents.default()
intents.message_content = True
intents.guilds = True

bot = commands.Bot(command_prefix='!', intents=intents)


@bot.event
async def on_ready():
    """Bot conectado."""
    print(f"🤖 Bot conectado como: {bot.user.name}")
    canal = bot.get_channel(DISCORD_ZONAS_CHANNEL_ID)
    if canal:
        print(f"✅ Canal encontrado: #{canal.name} en {canal.guild.name}")
        await enviar_telegram(
            f"✅ <b>Discord Zones Reader INICIADO</b>\n\n"
            f"Escuchando canal: #{canal.name}\n"
            f"⏰ {datetime.now().strftime('%H:%M:%S')}"
        )
    else:
        print(f"❌ No se encontro el canal {DISCORD_ZONAS_CHANNEL_ID}")


@bot.event
async def on_message(message: discord.Message):
    """Detecta imagenes en el canal de Trading Different."""
    
    if message.channel.id != DISCORD_ZONAS_CHANNEL_ID:
        return
    
    if message.author == bot.user:
        return
    
    if not message.attachments:
        return
    
    print(f"\n📨 Nueva imagen de {message.author.name}")
    
    for attachment in message.attachments:
        if not attachment.content_type or not attachment.content_type.startswith('image/'):
            continue
        
        print(f"🖼️ {attachment.filename} ({attachment.size} bytes)")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        image_filename = f"{timestamp}_{attachment.filename}"
        
        image_path = await descargar_imagen(attachment.url, image_filename)
        
        if image_path:
            print(f"🔍 Analizando con Gemini...")
            analisis = await analizar_imagen_gemini(image_path)
            
            json_filename = f"analisis_{timestamp}.json"
            guardar_analisis(analisis, json_filename)
            
            if es_activo_relevante(analisis):
                zonas = analisis.get("zonas_liquidez", [])
                activo = analisis.get("activo_detectado", "Desconocido")
                
                print(f"🎯 ACTIVO RELEVANTE! {activo} - {len(zonas)} zonas")
                
                msg = f"🖼️ <b>ZONAS DE LIQUIDEZ DETECTADAS</b>\n\n"
                msg += f"📊 Activo: <b>{activo}</b>\n"
                msg += f"🔍 Zonas encontradas: <b>{len(zonas)}</b>\n\n"
                
                for zona in zonas:
                    emoji = "🟢" if zona.get('tipo') == 'BUY_SIDE' else "🔴"
                    msg += f"{emoji} {zona.get('tipo')}: <b>{zona.get('rango')}</b>\n"
                    msg += f"   Confianza: {zona.get('confianza')}\n\n"
                
                msg += f"⏰ {datetime.now().strftime('%H:%M:%S')}"
                
                await enviar_telegram(msg)
                print(f"✅ Notificacion Telegram enviada")
                
                respuesta = f"✅ **Analisis completado**\n📊 {activo}: {len(zonas)} zonas detectadas"
                await message.reply(respuesta)
            else:
                print(f"ℹ️ Activo no relevante: {analisis.get('activo_detectado', 'Desconocido')}")


# ══════════════════════════════════════════════════════════
# ARRANQUE
# ══════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("╔══════════════════════════════════════════╗")
    print("║  DISCORD ZONES READER - INICIANDO       ║")
    print("╚══════════════════════════════════════════╝")
    
    if DISCORD_BOT_TOKEN == "PON_TU_BOT_TOKEN_AQUI":
        print("❌ ERROR: DISCORD_BOT_TOKEN no configurado")
        exit(1)
    
    if GEMINI_API_KEY == "PON_TU_API_KEY_DE_GOOGLE_AI_STUDIO_AQUI":
        print("❌ ERROR: GEMINI_API_KEY no configurada")
        exit(1)
    
    print(f"🎯 Simbolos: {SYMBOLS}")
    print(f"📡 Canal ID: {DISCORD_ZONAS_CHANNEL_ID}")
    print(f"🧠 Modelo: gemini-2.5-flash")
    print("🚀 Iniciando bot...")
    
    bot.run(DISCORD_BOT_TOKEN)
