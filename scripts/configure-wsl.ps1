[CmdletBinding()]
param(
    [ValidateRange(4, 64)]
    [int]$MemoryGB = 8,

    [ValidateRange(1, 32)]
    [int]$Processors = 4,

    [ValidateRange(0, 16)]
    [int]$SwapGB = 2
)

$ErrorActionPreference = "Stop"
$configPath = Join-Path $env:USERPROFILE ".wslconfig"
$content = @"
[wsl2]
memory=${MemoryGB}GB
processors=$Processors
swap=${SwapGB}GB
localhostForwarding=true

[experimental]
autoMemoryReclaim=gradual
"@

Set-Content -Path $configPath -Value $content -Encoding ASCII
Write-Host "Configuración WSL guardada en $configPath" -ForegroundColor Green
Write-Host "RAM: ${MemoryGB} GB | CPU: $Processors | Swap: ${SwapGB} GB" -ForegroundColor Cyan

# Docker Desktop debe estar cerrado para que el apagado sea limpio.
wsl --shutdown
Write-Host "WSL detenido. Abre Docker Desktop nuevamente para aplicar los límites." -ForegroundColor Green
