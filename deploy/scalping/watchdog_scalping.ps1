# Watchdog del SCALPING unicamente (puerto 8003). Tarea programada
# "ScalpingWatchdog", cada 5 min. Solo mira el puerto 8003 y solo mata el
# proceso que escucha ahi: jamas toca el swing (8002).
param(
    [string]$Root = "C:\bots\scalping",
    [int]$Port = 8003,
    [int]$Attempts = 3,
    [int]$RetrySleepSeconds = 30,
    [string]$RunScript = ""
)
$ErrorActionPreference = "Continue"
if ($Port -eq 8002) { throw "8002 es el puerto del swing: este watchdog no puede tocarlo" }
if (-not $RunScript) { $RunScript = Join-Path $Root "deploy\scalping\run_scalping.ps1" }

$logDir = Join-Path $Root "logs"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$logPath = Join-Path $logDir "watchdog.log"

function Write-Log($msg) {
    Add-Content -Path $logPath -Value "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')  $msg"
}

# Varios intentos espaciados antes de declarar caido el bot: una falla puntual
# (microcorte, pico de CPU) no dispara un reinicio.
function Test-BotHealthy {
    for ($i = 1; $i -le $Attempts; $i++) {
        $detail = ""
        try {
            $resp = Invoke-WebRequest -Uri "http://127.0.0.1:$Port/health" -TimeoutSec 15 -UseBasicParsing
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
        Write-Log "SCALPING health $i/$Attempts fallo: $detail"
        if ($i -lt $Attempts) { Start-Sleep -Seconds $RetrySleepSeconds }
    }
    return $false
}

function Stop-PortOwner {
    $conn = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
    foreach ($c in $conn) {
        try {
            Stop-Process -Id $c.OwningProcess -Force -ErrorAction Stop
            Write-Log "puerto $Port -- proceso $($c.OwningProcess) detenido"
        } catch {
            Write-Log "puerto $Port -- no se pudo detener proceso $($c.OwningProcess): $_"
        }
    }
}

if (-not (Test-BotHealthy)) {
    Write-Log "SCALPING no responde /health OK en :$Port -- reiniciando"
    Stop-PortOwner
    Start-Sleep -Seconds 3
    & $RunScript -Root $Root
    Write-Log "SCALPING relanzado"
}
