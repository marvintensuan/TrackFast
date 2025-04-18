param (
    [Parameter(Position=0)]
    [string]$Task
)

function Use-Venv {
    $venvPath = ".\.venv\Scripts\Activate.ps1"
    if (-Not (Test-Path $venvPath)) {
        Write-Error "Virtual environment not found at .venv/. Run 'python -m venv .venv' first."
        exit 1
    }

    Write-Host "Activating virtual environment..."
    & $venvPath
}

function Install {
    Use-Venv
    Write-Host "Installing dependencies from pyproject.toml..."

    # pip install --upgrade pip build ruff
    # pip install .
}

function Launch {
    Use-Venv

    $env:DAGSTER_HOME = [IO.Path]::Combine($(Get-Location), ".dagster_home")

    Write-Host "Launching Dagster UI..."
    dagster dev
}

function Build {
    Use-Venv
    Write-Host "Building wheel and sdist into ./dist..."
    # python -m build --outdir dist
}

function Lint {
    Use-Venv
    Write-Host "Running ruff linter..."
    ruff check TrackFast
}

function Show-Help {
    Write-Host "Usage: ./do.ps1 [command]"
    Write-Host ""
    Write-Host "Commands:"
    Write-Host "  install    Install dependencies and set up virtualenv"
    Write-Host "  launch     Start Dagster development server"
    Write-Host "  build      Build wheel and sdist into ./dist"
    Write-Host "  lint       Lint code with ruff"
    Write-Host "  help       Show this help message"
}

switch ($Task) {
    "install" { Install }
    "launch"  { Launch }
    "build"   { Build }
    "lint"    { Lint }
    "help"    { Show-Help }
    default   {
        if ($Task) {
            Write-Warning "Unknown command: $Task"
        }
        Show-Help
    }
}
