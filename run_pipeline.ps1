$ErrorActionPreference = "Stop"

Set-Location $PSScriptRoot

function Invoke-Step {
    param(
        [string]$Name,
        [string]$Executable,
        [string[]]$Arguments
    )

    Write-Host "`n>>> Starting: $Name"

    & $Executable @Arguments

    if ($LASTEXITCODE -ne 0) {
        throw "Step failed: $Name (exit code: $LASTEXITCODE)"
    }

    Write-Host ">>> Completed: $Name"
}

# 1. Generate Landing data
Invoke-Step "Landing" "python" @(
    "src/shipment_pulse/generator.py",
    "--output", "data/generated/landing",
    "--shipments", "20",
    "--seed", "42"
)

# 2. Build Spark Docker image
Invoke-Step "Docker build" "docker" @(
    "compose", "build", "spark"
)

# 3. Run Bronze
Invoke-Step "Bronze" "docker" @(
    "compose", "run", "--rm", "spark",
    "spark-submit",
    "/opt/project/src/shipment_pulse/bronze_ingestion.py"
)

# 4. Run Silver
$silverScripts = @(
    "silver_ports.py",
    "silver_shipments_cdc.py",
    "silver_shipment_events.py"
)

foreach ($script in $silverScripts) {
    Invoke-Step "Silver: $script" "docker" @(
        "compose", "run", "--rm", "spark",
        "spark-submit",
        "/opt/project/src/shipment_pulse/$script"
    )
}

# 5. Run Gold
Invoke-Step "Gold" "docker" @(
    "compose", "run", "--rm", "spark",
    "spark-pipelines", "run",
    "--spec", "/opt/project/spark-pipeline.yml"
)

# 6. Check Gold results
$sql = @"
SELECT
    s.shipment_count,
    r.route_count,
    assert_true(
        s.shipment_count = 20
        AND r.route_count = 16,
        'Gold row counts do not match expected values'
    ) AS validation
FROM
    (SELECT COUNT(*) AS shipment_count
     FROM default.shipment_summary) s
CROSS JOIN
    (SELECT COUNT(*) AS route_count
     FROM default.route_summary) r
"@

Invoke-Step "Gold validation" "docker" @(
    "compose", "run", "--rm", "spark",
    "spark-sql", "-e", $sql
)

Write-Host "`nPIPELINE FINISHED SUCCESSFULLY"