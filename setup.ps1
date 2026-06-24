# setup.ps1
param(
    [switch]$Backend,
    [switch]$Frontend
)

function Make-Venv($name) {
    python -m venv $name
    Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
    .\$name\Scripts\Activate.ps1
    python -m pip install --upgrade pip
}

if ($Backend) {
    Make-Venv backend_env
    pip install -r backend_requirements.txt
    deactivate
}
if ($Frontend) {
    Make-Venv frontend_env
    pip install -r frontend_requirements.txt
    deactivate
}