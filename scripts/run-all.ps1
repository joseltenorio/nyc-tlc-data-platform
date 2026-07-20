[CmdletBinding()]
param(
    [ValidateSet("historical", "incremental", "all")]
    [string]$Mode = "all",

    [switch]$SkipML,

    # 0 aplica un mínimo conservador según el modo.
    [int]$MinimumFreeGB = 0,

    # El pipeline se diseñó para una VM WSL limitada. Aumentar este valor es
    # explícito, no accidental.
    [ValidateRange(4, 64)]
    [int]$MaximumWSLMemoryGB = 8,

    [ValidateRange(1, 32)]
    [int]$MaximumWSLProcessors = 4
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

function Assert-WSLSafety {
    $wslConfig = Join-Path $env:USERPROFILE ".wslconfig"
    if (-not (Test-Path $wslConfig)) {
        throw "No existe $wslConfig. Ejecuta una vez: .\scripts\configure-wsl.ps1"
    }

    $content = Get-Content $wslConfig -Raw
    $memoryMatch = [regex]::Match($content, "(?im)^memory\s*=\s*(\d+)GB\s*$")
    $processorMatch = [regex]::Match($content, "(?im)^processors\s*=\s*(\d+)\s*$")
    if (-not $memoryMatch.Success -or -not $processorMatch.Success) {
        throw ".wslconfig debe declarar memory=<n>GB y processors=<n>. Ejecuta .\scripts\configure-wsl.ps1"
    }

    $memoryGB = [int]$memoryMatch.Groups[1].Value
    $processors = [int]$processorMatch.Groups[1].Value
    if ($memoryGB -gt $MaximumWSLMemoryGB) {
        throw "WSL tiene $memoryGB GB; el máximo autorizado por este comando es $MaximumWSLMemoryGB GB."
    }
    if ($processors -gt $MaximumWSLProcessors) {
        throw "WSL tiene $processors procesadores; el máximo autorizado es $MaximumWSLProcessors."
    }

    Write-Host "WSL seguro: ${memoryGB} GB RAM, $processors CPU." -ForegroundColor DarkGray
}

function Assert-DockerReady {
    & docker info *> $null
    if ($LASTEXITCODE -ne 0) {
        throw "Docker Desktop no está disponible. Ábrelo y espera a que el motor esté listo."
    }
}

function Get-FreeSpaceGB {
    $driveName = [System.IO.Path]::GetPathRoot($projectRoot).Substring(0, 1)
    $drive = Get-PSDrive -Name $driveName
    return [math]::Round($drive.Free / 1GB, 2)
}

function Assert-FreeSpace([int]$RequiredGB, [string]$Stage) {
    $freeGB = Get-FreeSpaceGB
    if ($freeGB -lt $RequiredGB) {
        throw "Espacio libre insuficiente antes de $($Stage): $freeGB GB. Se requieren al menos $RequiredGB GB."
    }
    Write-Host "Espacio libre antes de $($Stage): $freeGB GB" -ForegroundColor DarkGray
}

function Clear-SparkTemporaryData {
    $sparkTmp = Join-Path $projectRoot "data\tmp\spark"
    if (Test-Path $sparkTmp) {
        Get-ChildItem $sparkTmp -Force -ErrorAction SilentlyContinue |
            Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
    }
}

$requiredFreeGB = if ($MinimumFreeGB -gt 0) {
    $MinimumFreeGB
} elseif ($Mode -eq "incremental") {
    80
} else {
    250
}

Assert-WSLSafety
Assert-DockerReady
Assert-FreeSpace $requiredFreeGB "iniciar la plataforma"
New-Item -ItemType Directory -Force -Path ".\data\tmp\spark" | Out-Null

$compose = @("compose", "-f", "docker-compose.yml", "-f", "docker-compose.dashboard.yml")

# Un dashboard anterior puede consumir hasta 2 GB. Se detiene antes del build y
# durante todas las acciones Spark; vuelve a levantarse incluso si una capa falla.
& docker @compose stop dashboard 2>$null

Write-Host "[1/5] Construyendo imagen de pipeline y dashboard..." -ForegroundColor Cyan
& docker @compose build pipeline dashboard
if ($LASTEXITCODE -ne 0) { throw "Falló docker compose build." }

Write-Host "[2/5] Iniciando MongoDB..." -ForegroundColor Cyan
& docker @compose up -d mongodb
if ($LASTEXITCODE -ne 0) { throw "Falló el inicio de MongoDB." }

$pipelineFailure = $null
try {
    if ($Mode -in @("historical", "all")) {
        Assert-FreeSpace $requiredFreeGB "Bronze -> Silver -> Gold -> ML histórico"
        Write-Host "[3/5] Ejecutando alcance histórico" -ForegroundColor Cyan
        $historicalArgs = @("run", "--rm", "pipeline", "platform-historical")
        if ($SkipML) { $historicalArgs += "--no-train-ml" }
        & docker @compose @historicalArgs
        if ($LASTEXITCODE -ne 0) {
            throw "Falló platform-historical. Revisa auditoría y manifiestos; Gold/ML no publican salidas incompletas."
        }
        Clear-SparkTemporaryData
    }

    if ($Mode -in @("incremental", "all")) {
        Assert-FreeSpace 80 "actualización incremental 2026"
        Write-Host "[4/5] Ejecutando 2026 disponible para Yellow, Green y FHV..." -ForegroundColor Cyan
        & docker @compose run --rm pipeline platform-incremental --no-train-ml
        if ($LASTEXITCODE -ne 0) {
            throw "Falló platform-incremental. Revisa auditoría y manifiestos."
        }
        Clear-SparkTemporaryData
    }
}
catch {
    $pipelineFailure = $_
}
finally {
    # El spill queda en el bind mount data/tmp/spark, no oculto en docker_data.vhdx.
    Clear-SparkTemporaryData
}

Write-Host "[5/5] Iniciando Streamlit..." -ForegroundColor Cyan
& docker @compose up -d dashboard
if ($LASTEXITCODE -ne 0) { throw "Falló el inicio del dashboard." }

Write-Host "Dashboard disponible en http://localhost:8501" -ForegroundColor Green
if ($null -ne $pipelineFailure) {
    throw $pipelineFailure
}
Write-Host "Pipeline finalizado correctamente." -ForegroundColor Green
