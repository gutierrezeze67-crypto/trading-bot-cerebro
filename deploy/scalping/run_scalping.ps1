# Lanza el bot de SCALPING desde su propia instalacion (C:\bots\scalping): su
# carpeta, su venv, su .env y sus logs. No comparte nada con el swing.
param([string]$Root = "C:\bots\scalping")
$ErrorActionPreference = "Stop"

$bot  = Join-Path $Root "unified_brain"
$py   = Join-Path $Root "venv\Scripts\python.exe"
$logs = Join-Path $Root "logs"

if (-not (Test-Path $py)) { throw "Falta $py -- correr install_scalping.ps1 primero" }
if (-not (Test-Path (Join-Path $bot ".env"))) { throw "Falta $bot\.env" }
New-Item -ItemType Directory -Force -Path $logs | Out-Null

# La configuracion sale SOLO del .env de esta carpeta. Se borran del entorno de
# esta sesion las variables que hayan quedado de lanzamientos manuales previos
# (python-dotenv no pisa lo que ya esta seteado, y asi se colaba configuracion
# de otro bot -- pasó el 2026-09-09).
Get-ChildItem Env: |
    Where-Object { $_.Name -match '^(UNIFIED_BRAIN_|RISK_|MT5_|MT5MCP_|SCALPING_)' -or $_.Name -eq 'BACKEND_API_KEY' } |
    ForEach-Object { Remove-Item -Path "Env:\$($_.Name)" }

$out = Join-Path $logs "scalping-out.log"
$err = Join-Path $logs "scalping-err.log"
Start-Process -FilePath "cmd.exe" `
    -ArgumentList "/c set PYTHONUNBUFFERED=1 && `"$py`" -u main_orchestrator.py >> `"$out`" 2>> `"$err`"" `
    -WorkingDirectory $bot -WindowStyle Hidden
