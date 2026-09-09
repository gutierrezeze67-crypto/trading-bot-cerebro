# Lanza el segundo proceso (scalping, cuenta demo Exness Zero) desde esta
# misma carpeta de unified_brain/. Usa su propio archivo de secretos
# (.env.exness_zero_demo_200, NO versionado en git, crear una vez en el VPS
# a partir de .env.exness_zero_demo_200.example) en vez de variables de
# entorno tipeadas a mano en la sesion de PowerShell -- asi watchdog.ps1
# puede relanzarlo solo sin que nadie tenga que volver a tipear la password.
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

$envFile = Join-Path $PSScriptRoot ".env.exness_zero_demo_200"
if (-not (Test-Path $envFile)) {
    throw "Falta $envFile -- copiar .env.exness_zero_demo_200.example a ese nombre y completar MT5_LOGIN/MT5_PASSWORD/MT5_SERVER/MT5_TERMINAL_PATH/BACKEND_API_KEY una sola vez."
}

$env:UNIFIED_BRAIN_ENV_FILE = ".env.exness_zero_demo_200"

Start-Process -FilePath "cmd.exe" -ArgumentList '/c set PYTHONUNBUFFERED=1 && set UNIFIED_BRAIN_ENV_FILE=.env.exness_zero_demo_200 && "C:\Program Files\Python311\python.exe" -u main_orchestrator.py >> "bot_log.txt" 2>> "bot_error.txt"' -WorkingDirectory $PSScriptRoot -WindowStyle Hidden
