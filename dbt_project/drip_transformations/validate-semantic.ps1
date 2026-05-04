param(
    [switch]$PartialParse
)

$ErrorActionPreference = "Stop"

$dbtPath = ".\.venv-mf311\Scripts\dbt.exe"
$pythonPath = ".\.venv-mf311\Scripts\python.exe"

if (-not (Test-Path $dbtPath)) {
    throw "Missing dbt executable at $dbtPath. Create the .venv-mf311 environment first."
}

if (-not (Test-Path $pythonPath)) {
    throw "Missing python executable at $pythonPath. Create the .venv-mf311 environment first."
}

$parseArgs = @("parse")
if (-not $PartialParse) {
    $parseArgs += "--no-partial-parse"
}

Write-Host "Running dbt parse..."
& $dbtPath @parseArgs
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

$validator = @"
from dbt_metricflow.cli.cli_configuration import CLIConfiguration
from dbt_metricflow.cli.dbt_connectors.dbt_config_accessor import dbtArtifacts
from metricflow_semantic_interfaces.validations.semantic_manifest_validator import SemanticManifestValidator

cfg = CLIConfiguration()
cfg.setup()
manifest = dbtArtifacts.build_semantic_manifest_from_dbt_project_root(cfg.dbt_project_metadata.project_path)
result = SemanticManifestValidator(max_workers=1).validate_semantic_manifest(manifest)

print('blocking_issues=', result.has_blocking_issues)
print('errors=', len(result.errors), 'warnings=', len(result.warnings), 'future_errors=', len(result.future_errors))
"@

Write-Host "Running semantic validation..."
& $pythonPath -c $validator
exit $LASTEXITCODE
