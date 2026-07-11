"""
╔══════════════════════════════════════════════════════════╗
║  TRADING PLATFORM - CONFIGURACIÓN CENTRAL (TEMPLATE)     ║
║  Copiá este archivo a settings.py y poné tus datos reales║
║  settings.py está en .gitignore - nunca se commitea.     ║
╚══════════════════════════════════════════════════════════╝
"""

# ══════════════════════════════════════════════════════════
# 🔑 SECCIÓN DE CLAVES API - PON TUS DATOS REALES AQUÍ
# ══════════════════════════════════════════════════════════

# Gemini AI (Google AI Studio) - generar en https://aistudio.google.com/apikey
GEMINI_API_KEY = "PON_TU_API_KEY_DE_GOOGLE_AI_STUDIO_AQUI"

# Telegram (para recibir alertas en tu móvil) - crear bot con @BotFather
TELEGRAM_BOT_TOKEN = "PON_TU_TELEGRAM_BOT_TOKEN_AQUI"
TELEGRAM_CHAT_ID = "PON_TU_TELEGRAM_CHAT_ID_AQUI"

# Discord Bot (para LEER imágenes del canal de Trading Different)
DISCORD_BOT_TOKEN = "PON_TU_DISCORD_BOT_TOKEN_AQUI"

# Discord Webhook (para ENVIAR notificaciones de alertas)
DISCORD_WEBHOOK_URL = "PON_TU_WEBHOOK_AQUI"

# Canal de Discord donde están las imágenes de Trading Different
DISCORD_ZONAS_CHANNEL_ID = 0

# ══════════════════════════════════════════════════════════
# 📊 SÍMBOLOS DE TRADING (los que quieres monitorear)
# ══════════════════════════════════════════════════════════
SYMBOLS = [
    "BTCUSDT",
    "XAUUSDT",
    "XAGUSDT",
    "USOIL",
    "US30",
    "SP500",
    "NAS100",
]

# ══════════════════════════════════════════════════════════
# ⚙️ CONFIGURACIÓN GENERAL
# ══════════════════════════════════════════════════════════
PRIMARY_TF = "15m"
DAILY_LOSS_LIMIT_PERCENT = 5.0
MAX_POSITIONS = 3

# ══════════════════════════════════════════════════════════
# 🔍 CONFIGURACIÓN DEL SCANNER SENSEI V3
# ══════════════════════════════════════════════════════════
SCANNER_CONFIG = {
    "hh_ll_period": 70,
    "bos_lookback": 20,
    "pivot_len": 3,
    "cont_alta_th": 50.0,
    "cont_baja_th": 55.0,
    "choch_window": 20,
    "min_prob_ob": 50,
    "max_ob_activos": 8,
    "max_fvg_activos": 10,
    "historical_zones": 90,
    "min_risk_atr": 0.5,
    "require_disp": False,
    "disp_multiplier": 1.2,
    "atr_length": 14,
    "w1": 0.35,
    "w2": 0.25,
    "w3": 0.25,
    "w4": 0.15,
}

# ══════════════════════════════════════════════════════════
# 🤖 CONFIGURACIÓN GEMINI 2.5 FLASH
# ══════════════════════════════════════════════════════════
GEMINI_MODES = {
    "monitoreo": {
        "temperature": 0.3,
        "max_tokens": 500,
        "system_prompt": "Eres un monitor de trading. Analiza brevemente. Sé conciso."
    },
    "mentoria": {
        "temperature": 0.6,
        "max_tokens": 1000,
        "system_prompt": "Eres un mentor de trading experto. Explica y ayuda a mejorar."
    },
    "analisis_profundo": {
        "temperature": 0.4,
        "max_tokens": 2000,
        "system_prompt": "Analista profesional. Análisis técnico detallado con zonas, SVP, fractales."
    }
}
