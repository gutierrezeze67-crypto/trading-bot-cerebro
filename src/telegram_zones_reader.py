"""
╔══════════════════════════════════════════════════════════╗
║  TELEGRAM ZONES READER - Recibe imagenes por Telegram   ║
║  Las analiza con Gemini 2.5 Flash                       ║
╚══════════════════════════════════════════════════════════╝

Flujo:
1. Alguien te envia una imagen de zonas de liquidez por Telegram
2. El bot la descarga
3. Gemini 2.5 Flash extrae las zonas con precios
4. Te responde con las zonas detectadas
5. Guarda todo en data/analisis_zonas/
"""

import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import google.generativeai as genai
import aiohttp

from config.settings import (
    TELEGRAM_BOT_TOKEN,
    GEMINI_API_KEY,
    SYMBOLS,
)

# ══════════════════════════════════════════════════════════
# CONFIGURACION
# ══════════════════════════════════════════════════════════

genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-2.5-flash')

DATA_DIR = Path(__file__).parent.parent / "data"
IMAGES_DIR = DATA_DIR / "imagenes_zonas"
ANALISIS_DIR = DATA_DIR / "analisis_zonas"

IMAGES_DIR.mkdir(parents=True, exist_ok=True)
ANALISIS_DIR.mkdir(parents=True, exist_ok=True)

TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
ULTIMO_UPDATE_ID = 0

# ══════════════════════════════════════════════════════════
# FUNCIONES
# ══════════════════════════════════════════════════════════

async def descargar_archivo_telegram(file_id: str, filename: str) -> Path:
    """Descarga un archivo de Telegram."""
    # Obtener info del archivo
    async with aiohttp.ClientSession() as session:
        url = f"{TELEGRAM_API}/getFile?file_id={file_id}"
        async with session.get(url) as resp:
            data = await resp.json()
            if not data.get("ok"):
                print(f"❌ Error getFile: {data}")
                return None
            file_path = data["result"]["file_path"]
        
        # Descargar
        download_url = f"https://api.telegram.org/file/bot{TELEGRAM_BOT_TOKEN}/{file_path}"
        filepath = IMAGES_DIR / filename
        async with session.get(download_url) as resp:
            if resp.status == 200:
                data = await resp.read()
                with open(filepath, 'wb') as f:
                    f.write(data)
                print(f"✅ Imagen descargada: {filename}")
                return filepath
            else:
                print(f"❌ Error descarga: {resp.status}")
                return None


async def analizar_imagen_gemini(image_path: Path) -> dict:
    """Envia imagen a Gemini para extraer zonas."""
    with open(image_path, 'rb') as f:
        image_data = f.read()
    
    image_part = {
        "mime_type": "image/png",
        "data": image_data
    }
    
    prompt = """
Eres un analista de zonas de liquidez profesional. Analiza esta imagen de Trading Different.

INSTRUCCIONES:
1. Busca TODOS los rectangulos dibujados (zonas de liquidez)
2. Para cada rectangulo, extrae EXACTAMENTE:
   - Precio superior (borde de arriba)
   - Precio inferior (borde de abajo)
3. Identifica el activo/simbolo
4. Determina si es COMPRA (buy side) o VENTA (sell side)

FORMATO DE RESPUESTA (JSON):
{
  "activo_detectado": "XAUUSD",
  "zonas_liquidez": [
    {
      "tipo": "BUY_SIDE",
      "precio_superior": 2650.50,
      "precio_inferior": 2645.00,
      "rango": "2645.00 - 2650.50",
      "confianza": "ALTA"
    }
  ],
  "resumen": "1 zona de compra detectada"
}

Si no hay rectangulos: {"zonas_liquidez": [], "resumen": "Sin zonas"}
"""
    
    try:
        response = model.generate_content([prompt, image_part])
        texto = response.text
        
        if "```json" in texto:
            texto = texto.split("```json")[1].split("```")[0].strip()
        elif "```" in texto:
            texto = texto.split("```")[1].split("```")[0].strip()
        
        analisis = json.loads(texto)
        analisis["imagen_original"] = str(image_path.name)
        analisis["timestamp"] = datetime.now().isoformat()
        
        zonas = analisis.get("zonas_liquidez", [])
        print(f"✅ {len(zonas)} zonas detectadas")
        return analisis
        
    except Exception as e:
        print(f"❌ Error Gemini: {e}")
        return {"error": str(e), "imagen_original": str(image_path.name)}


async def enviar_telegram(chat_id: str, mensaje: str):
    """Envia mensaje a Telegram."""
    url = f"{TELEGRAM_API}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": mensaje,
        "parse_mode": "HTML"
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as resp:
            return await resp.json()


def guardar_analisis(analisis: dict, filename: str):
    """Guarda analisis en JSON."""
    filepath = ANALISIS_DIR / filename
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(analisis, f, indent=2, ensure_ascii=False)
    print(f"💾 Guardado: {filename}")


# ══════════════════════════════════════════════════════════
# BUCLE PRINCIPAL - Revisa mensajes cada 3 segundos
# ══════════════════════════════════════════════════════════

async def main():
    global ULTIMO_UPDATE_ID
    
    print("╔══════════════════════════════════════════╗")
    print("║  TELEGRAM ZONES READER - INICIANDO      ║")
    print("╚══════════════════════════════════════════╝")
    print(f"🧠 Modelo: gemini-2.5-flash")
    print(f"📡 Esperando imágenes por Telegram...")
    print(f"💡 Envía una imagen al bot para analizarla\n")
    
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                # Obtener updates
                url = f"{TELEGRAM_API}/getUpdates?offset={ULTIMO_UPDATE_ID + 1}&timeout=5"
                async with session.get(url) as resp:
                    data = await resp.json()
                
                if not data.get("ok") or not data["result"]:
                    await asyncio.sleep(2)
                    continue
                
                for update in data["result"]:
                    ULTIMO_UPDATE_ID = update["update_id"]
                    message = update.get("message", {})
                    
                    # Solo procesar si tiene foto
                    if "photo" not in message:
                        continue
                    
                    chat_id = str(message["chat"]["id"])
                    username = message["from"].get("username", message["from"].get("first_name", "Desconocido"))
                    
                    print(f"\n📨 Imagen de {username}")
                    
                    # Telegram envia varias resoluciones, tomamos la mas grande
                    photo = message["photo"][-1]
                    file_id = photo["file_id"]
                    
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    filename = f"telegram_{timestamp}.jpg"
                    
                    # Descargar
                    image_path = await descargar_archivo_telegram(file_id, filename)
                    
                    if image_path:
                        print("🔍 Analizando con Gemini...")
                        analisis = await analizar_imagen_gemini(image_path)
                        
                        # Guardar
                        json_filename = f"analisis_{timestamp}.json"
                        guardar_analisis(analisis, json_filename)
                        
                        # Responder
                        zonas = analisis.get("zonas_liquidez", [])
                        activo = analisis.get("activo_detectado", "Desconocido")
                        
                        if zonas:
                            msg = f"🖼️ <b>ZONAS DE LIQUIDEZ DETECTADAS</b>\n\n"
                            msg += f"📊 Activo: <b>{activo}</b>\n"
                            msg += f"🔍 Zonas: <b>{len(zonas)}</b>\n\n"
                            
                            for zona in zonas:
                                emoji = "🟢" if zona.get('tipo') == 'BUY_SIDE' else "🔴"
                                msg += f"{emoji} {zona.get('tipo')}: <b>{zona.get('rango')}</b>\n"
                                msg += f"   Confianza: {zona.get('confianza')}\n\n"
                        else:
                            msg = f"⚠️ No se detectaron zonas de liquidez en la imagen."
                        
                        msg += f"⏰ {datetime.now().strftime('%H:%M:%S')}"
                        
                        await enviar_telegram(chat_id, msg)
                        print(f"✅ Respuesta enviada a {username}")
                
            except Exception as e:
                print(f"❌ Error: {e}")
                await asyncio.sleep(3)


if __name__ == "__main__":
    asyncio.run(main())
