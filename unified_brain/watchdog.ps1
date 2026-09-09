# Watchdog real de los 2 procesos de unified_brain (swing puerto 8002,
# scalping puerto 8003). Se registra en el Programador de Tareas como
# "UnifiedBrainWatchdog" corriendo cada 5 min.
#
# ANTES este script solo chequeaba "existe algun proceso llamado python" --
# eso NUNCA detecta que la conexion Python-MT5 murio por dentro sin que el
# proceso se caiga (confirmado en vivo 2026-08-31/09-09: 9 dias sin operar,
# /health devolviendo "ok" fijo todo el tiempo, python.exe vivo todo el
# tiempo). Ahora chequea /health de cada puerto -- que si refleja la
# conexion MT5 real desde el fix en api.py -- y reinicia SOLO el bot que
# esta roto, no los dos a ciegas.
$ErrorActionPreference = "Continue"
$root = "C:\trading-bot-cerebro\unified_brain"
Set-Location $root
$logPath = Join-Path $root "watchdog.log"

function Write-Log($msg) {
    $line = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')  $msg"
    Add-Content -Path $logPath -Value $line
}

function Test-BotHealthy($port) {
    try {
        $resp = Invoke-WebRequest -Uri "http://127.0.0.1:$port/health" -TimeoutSec 5 -UseBasicParsing
        if ($resp.StatusCode -ne 200) { return $false }
        $body = $resp.Content | ConvertFrom-Json
        return $body.status -eq "ok"
    } catch {
        return $false
    }
}

function Stop-PortOwner($port) {
    $conn = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue
    foreach ($c in $conn) {
        try {
            Stop-Process -Id $c.OwningProcess -Force -ErrorAction Stop
            Write-Log "puerto $port -- proceso $($c.OwningProcess) detenido"
        } catch {
            Write-Log "puerto $port -- no se pudo detener proceso $($c.OwningProcess): $_"
        }
    }
}

# --- Swing (Vantage, puerto 8002) ---
if (-not (Test-BotHealthy 8002)) {
    Write-Log "SWING no responde /health OK en :8002 -- reiniciando"
    Stop-PortOwner 8002
    Start-Sleep -Seconds 3
    Start-Process -FilePath "cmd.exe" -ArgumentList '/c set PYTHONUNBUFFERED=1 && "C:\Program Files\Python311\python.exe" -u main_orchestrator.py >> "service-out.log" 2>> "service-err.log"' -WorkingDirectory $root -WindowStyle Hidden
    Write-Log "SWING relanzado"
}

# --- Scalping (Exness, puerto 8003) -- solo si su archivo de secretos existe ---
$scalpingEnvFile = Join-Path $root ".env.exness_zero_demo_200"
if (Test-Path $scalpingEnvFile) {
    if (-not (Test-BotHealthy 8003)) {
        Write-Log "SCALPING no responde /health OK en :8003 -- reiniciando"
        Stop-PortOwner 8003
        Start-Sleep -Seconds 3
        & (Join-Path $root "start_scalping.ps1")
        Write-Log "SCALPING relanzado"
    }
}
