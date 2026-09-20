# Prepara la instalacion INDEPENDIENTE del scalping en C:\bots\scalping.
# NO arranca ningun bot, NO abre conexion a MT5 y NO toca el swing: solo crea
# el venv propio, copia el .env del scalping y comprueba que el codigo importa.
param(
    [string]$Root = "C:\bots\scalping",
    [string]$OldEnv = "C:\trading-bot-cerebro\unified_brain\.env.exness_zero_demo_200",
    [string]$Python = "C:\Program Files\Python311\python.exe"
)
$ErrorActionPreference = "Stop"

function Fail($msg) { Write-Host "ERROR: $msg" -ForegroundColor Red; exit 1 }

$bot  = Join-Path $Root "unified_brain"
$venv = Join-Path $Root "venv"
$py   = Join-Path $venv "Scripts\python.exe"

if (-not (Test-Path (Join-Path $bot "main_orchestrator.py"))) { Fail "no encuentro $bot\main_orchestrator.py (falta el git clone)" }
if (-not (Test-Path $Python)) { Fail "no encuentro $Python" }

# 1) Entorno de Python propio. --system-site-packages: hereda las MISMAS
#    versiones de librerias ya instaladas (las validadas), sin descargar nada;
#    todo lo que se instale desde ahora en este venv queda SOLO aca.
if (-not (Test-Path $py)) {
    & $Python -m venv --system-site-packages $venv
    if ($LASTEXITCODE -ne 0 -or -not (Test-Path $py)) { Fail "no se pudo crear el venv" }
    Write-Host "venv creado: $venv"
} else {
    Write-Host "venv ya existe: $venv"
}

# 2) .env propio (copia del archivo de secretos que usaba el scalping).
$envDst = Join-Path $bot ".env"
if (-not (Test-Path $envDst)) {
    if (-not (Test-Path $OldEnv)) { Fail "no existe $OldEnv ni $envDst -- crear $envDst con los datos de la cuenta Exness" }
    Copy-Item $OldEnv $envDst
    Write-Host ".env copiado desde $OldEnv"
} else {
    Write-Host ".env ya existe (no se pisa)"
}

# 2b) config\settings.py no viaja por git (tiene claves de Gemini/Telegram/
#     Discord) pero order_flow_signal.py lo importa al arrancar. El camino
#     determinista del scalping no usa ninguna de esas claves, asi que se crea
#     desde la plantilla versionada: esta instalacion no lleva claves ajenas.
$settingsDst = Join-Path $Root "config\settings.py"
$settingsTpl = Join-Path $Root "config\settings.example.py"
if (-not (Test-Path $settingsDst)) {
    if (-not (Test-Path $settingsTpl)) { Fail "falta $settingsTpl" }
    Copy-Item $settingsTpl $settingsDst
    Write-Host "config\settings.py creado desde la plantilla (sin claves)"
}

# 3) Validar identidad del .env sin imprimir secretos.
$cfg = @{}
foreach ($line in Get-Content $envDst) {
    if ($line -match '^\s*([A-Za-z0-9_]+)\s*=\s*(.*?)\s*$' -and -not $line.TrimStart().StartsWith('#')) { $cfg[$Matches[1]] = $Matches[2] }
}
foreach ($k in 'UNIFIED_BRAIN_USER_ID','UNIFIED_BRAIN_PORT','MT5_LOGIN','MT5_SERVER','MT5_PASSWORD','MT5_TERMINAL_PATH','BACKEND_API_KEY') {
    if (-not $cfg[$k]) { Fail "falta $k en $envDst" }
}
if ($cfg['UNIFIED_BRAIN_PORT'] -ne '8003') { Fail "UNIFIED_BRAIN_PORT es $($cfg['UNIFIED_BRAIN_PORT']), tiene que ser 8003 (8002 es del swing)" }
if ($cfg['UNIFIED_BRAIN_ENABLE_SWING'] -and $cfg['UNIFIED_BRAIN_ENABLE_SWING'].ToLower() -ne 'false') { Fail "UNIFIED_BRAIN_ENABLE_SWING tiene que ser false en el scalping" }
if (-not (Test-Path $cfg['MT5_TERMINAL_PATH'])) { Fail "no existe la terminal MT5: $($cfg['MT5_TERMINAL_PATH'])" }
# El scalping se enciende de forma explicita (no por "default").
if (-not $cfg['UNIFIED_BRAIN_ENABLE_SCALPING']) {
    Add-Content -Path $envDst -Value "`r`nUNIFIED_BRAIN_ENABLE_SCALPING=true"
    Write-Host "agregado UNIFIED_BRAIN_ENABLE_SCALPING=true"
}
Write-Host ("identidad: user_id={0} puerto={1} cuenta={2} servidor={3}" -f $cfg['UNIFIED_BRAIN_USER_ID'], $cfg['UNIFIED_BRAIN_PORT'], $cfg['MT5_LOGIN'], $cfg['MT5_SERVER'])

# 4) Prueba de importacion (no conecta a MT5, no abre puertos), con el entorno
#    de esta sesion limpio para que solo cuente el .env.
Get-ChildItem Env: |
    Where-Object { $_.Name -match '^(UNIFIED_BRAIN_|RISK_|MT5_|MT5MCP_|SCALPING_)' -or $_.Name -eq 'BACKEND_API_KEY' } |
    ForEach-Object { Remove-Item -Path "Env:\$($_.Name)" }
$tmpOut = Join-Path $env:TEMP "scalping_import_out.txt"
$tmpErr = Join-Path $env:TEMP "scalping_import_err.txt"
$p = Start-Process -FilePath $py -ArgumentList '-c "import main_orchestrator; print(''IMPORT_OK'')"' -WorkingDirectory $bot `
    -Wait -PassThru -WindowStyle Hidden -RedirectStandardOutput $tmpOut -RedirectStandardError $tmpErr
$out = Get-Content $tmpOut -Raw
if ($p.ExitCode -ne 0 -or $out -notmatch 'IMPORT_OK') {
    Write-Host (Get-Content $tmpErr -Tail 15 | Out-String)
    Fail "el codigo no importa (ver mensaje de arriba)"
}
Write-Host "import OK -- instalacion lista (todavia NO arrancada)" -ForegroundColor Green
exit 0
