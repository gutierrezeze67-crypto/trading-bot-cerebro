# Registra la tarea programada "ScalpingWatchdog" (cada 5 min) que corre
# watchdog_scalping.ps1. Copia usuario y ajustes de la tarea del swing
# (UnifiedBrainWatchdog) para que corra en el mismo contexto que ya puede hablar
# con las terminales MT5. No modifica ninguna tarea existente.
param(
    [string]$Root = "C:\bots\scalping",
    [string]$TaskName = "ScalpingWatchdog",
    [string]$Reference = "UnifiedBrainWatchdog"
)
$ErrorActionPreference = "Stop"
function Fail($msg) { Write-Host "ERROR: $msg" -ForegroundColor Red; exit 1 }

$script = Join-Path $Root "deploy\scalping\watchdog_scalping.ps1"
if (-not (Test-Path $script)) { Fail "no existe $script" }
if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) { Fail "la tarea $TaskName ya existe (no se pisa)" }

$ref = Get-ScheduledTask -TaskName $Reference -ErrorAction SilentlyContinue
$action  = New-ScheduledTaskAction -Execute "powershell.exe" -Argument "-WindowStyle Hidden -ExecutionPolicy Bypass -File `"$script`""
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(2) -RepetitionInterval (New-TimeSpan -Minutes 5) -RepetitionDuration (New-TimeSpan -Days 3650)

try {
    if (-not $ref) { throw "no existe la tarea de referencia $Reference" }
    Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Principal $ref.Principal -Settings $ref.Settings | Out-Null
} catch {
    Write-Host "no se pudo copiar el contexto de $Reference ($_) -- registro con el usuario actual" -ForegroundColor Yellow
    $settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -StartWhenAvailable
    Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings -RunLevel Highest | Out-Null
}
$t = Get-ScheduledTask -TaskName $TaskName
Write-Host ("tarea {0} registrada: usuario={1} estado={2}" -f $TaskName, $t.Principal.UserId, $t.State) -ForegroundColor Green
exit 0
