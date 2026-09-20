# Pasa el scalping a su instalacion independiente: detiene SOLO lo que escucha
# en el puerto 8003 (el scalping viejo), lanza el nuevo desde C:\bots\scalping
# y espera a que /health responda ok. Al final muestra el health de ambos bots
# (solo lectura). NUNCA toca el puerto 8002 (swing).
param(
    [string]$Root = "C:\bots\scalping",
    [int]$Port = 8003,
    [int]$WaitSeconds = 120
)
$ErrorActionPreference = "Stop"
function Fail($msg) { Write-Host "ERROR: $msg" -ForegroundColor Red; exit 1 }
if ($Port -eq 8002) { Fail "8002 es el puerto del swing" }

$bot = Join-Path $Root "unified_brain"
if (-not (Test-Path (Join-Path $Root "venv\Scripts\python.exe"))) { Fail "falta el venv -- correr install_scalping.ps1" }
if (-not (Test-Path (Join-Path $bot ".env"))) { Fail "falta $bot\.env -- correr install_scalping.ps1" }

# 1) Detener solo el que escucha en el puerto del scalping.
foreach ($c in @(Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue)) {
    Stop-Process -Id $c.OwningProcess -Force
    Write-Host "scalping viejo detenido (PID $($c.OwningProcess))"
}
Start-Sleep -Seconds 3

# 2) Lanzar el nuevo desde su carpeta.
& (Join-Path $Root "deploy\scalping\run_scalping.ps1") -Root $Root
Write-Host "scalping nuevo lanzado, esperando /health..."

# 3) Esperar /health ok.
$ok = $false
$deadline = (Get-Date).AddSeconds($WaitSeconds)
while ((Get-Date) -lt $deadline) {
    Start-Sleep -Seconds 5
    try {
        $r = Invoke-WebRequest -Uri "http://127.0.0.1:$Port/health" -TimeoutSec 5 -UseBasicParsing
        if ($r.StatusCode -eq 200 -and ($r.Content | ConvertFrom-Json).status -eq "ok") { $ok = $true; break }
    } catch {}
}

# 4) Resumen de ambos bots (solo lectura).
foreach ($p in 8002, $Port) {
    try { $h = (Invoke-WebRequest -Uri "http://127.0.0.1:$p/health" -TimeoutSec 5 -UseBasicParsing).Content } catch { $h = "SIN RESPUESTA" }
    Write-Host "health :$p -> $h"
}
$log = Join-Path $Root "logs\scalping-out.log"
if (Test-Path $log) {
    Write-Host "--- arranque del scalping ---"
    Select-String -Path $log -Pattern "scalping_expert_(en|dis)abled|scalping_validated_constants_applied|scalping_disabled" | Select-Object -Last 4 | ForEach-Object { $_.Line }
}
if (-not $ok) { Fail "el scalping nuevo no respondio /health ok en $WaitSeconds s -- ver $Root\logs" }
Write-Host "cutover OK" -ForegroundColor Green
exit 0
