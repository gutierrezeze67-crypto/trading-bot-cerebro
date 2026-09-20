# Watchdog del SWING unicamente (puerto 8002). Se registra en el Programador de
# Tareas como "UnifiedBrainWatchdog" corriendo cada 5 min.
#
# El scalping (puerto 8003) vive en su propia instalacion (C:\bots\scalping) con
# su propio watchdog: este script NO lo mira, NO lo reinicia y NO lo lanza.
# Rama swing-prod: el codigo del swing solo cambia por decision explicita, nunca
# por un `git pull` del trabajo de scalping.
#
# Chequea /health -- que refleja la conexion MT5 real (ver api.py) -- y reinicia
# el bot solo tras 3 fallas seguidas espaciadas.
$ErrorActionPreference = "Continue"
$root = "C:\trading-bot-cerebro\unified_brain"
Set-Location $root
$logPath = Join-Path $root "watchdog.log"

function Write-Log($msg) {
    $line = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')  $msg"
    Add-Content -Path $logPath -Value $line
}

# 3 intentos espaciados 30s antes de declarar caido un bot: una falla puntual
# (microcorte, pico de CPU del VPS) ya no dispara un reinicio; una falla real
# (MT5 colgado) igual se detecta, con ~1 min extra. Cada intento fallido deja
# el motivo exacto en watchdog.log.
function Test-BotHealthy($port, $name) {
    $attempts = 3
    for ($i = 1; $i -le $attempts; $i++) {
        $detail = ""
        try {
            $resp = Invoke-WebRequest -Uri "http://127.0.0.1:$port/health" -TimeoutSec 15 -UseBasicParsing
            $body = $resp.Content | ConvertFrom-Json
            if ($resp.StatusCode -eq 200 -and $body.status -eq "ok") { return $true }
            $detail = "HTTP $($resp.StatusCode) $($resp.Content)"
        } catch {
            $detail = $_.Exception.Message
            $r = $_.Exception.Response
            if ($r) {
                try {
                    $sr = New-Object System.IO.StreamReader($r.GetResponseStream())
                    $detail = "$detail | $($sr.ReadToEnd())"
                } catch {}
            }
        }
        Write-Log "$name health $i/$attempts fallo: $detail"
        if ($i -lt $attempts) { Start-Sleep -Seconds 30 }
    }
    return $false
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
if (-not (Test-BotHealthy 8002 "SWING")) {
    Write-Log "SWING no responde /health OK en :8002 -- reiniciando"
    Stop-PortOwner 8002
    Start-Sleep -Seconds 3
    Start-Process -FilePath "cmd.exe" -ArgumentList '/c set PYTHONUNBUFFERED=1 && "C:\Program Files\Python311\python.exe" -u main_orchestrator.py >> "service-out.log" 2>> "service-err.log"' -WorkingDirectory $root -WindowStyle Hidden
    Write-Log "SWING relanzado"
}
