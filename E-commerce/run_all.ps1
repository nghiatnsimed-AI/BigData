# ===============================================================
# RUN_ALL.PS1 — Chạy toàn bộ pipeline Big Data theo đúng thứ tự
# ===============================================================
# Cách dùng:
#   .\run_all.ps1              → chạy cả 8 bước
#   .\run_all.ps1 -From 4      → chạy lại từ bước 4 (Data_Silver) trở đi
#   .\run_all.ps1 -Only gold   → chạy đúng 1 bước theo tên
#
# Tên bước: collect, cleaning, cdc, silver, verify, gold, dashboard, architecture
# ===============================================================

param(
    [int]$From = 1,
    [string]$Only = ""
)

$ErrorActionPreference = "Continue"
Set-Location $PSScriptRoot

$PY = ".\.venv311\Scripts\python.exe"
$LOGDIR = "Data\logs"

# --- Kiểm tra môi trường trước khi chạy ---
function Test-Env {
    $ok = $true
    if (-not (Test-Path $PY)) {
        Write-Host "[FAIL] Khong tim thay $PY - hay tao venv Python 3.11 truoc" -ForegroundColor Red
        $ok = $false
    }
    if (-not $env:JAVA_HOME) {
        Write-Host "[FAIL] JAVA_HOME chua duoc dat" -ForegroundColor Red
        $ok = $false
    }
    if (-not (Test-Path "$env:JAVA_HOME\bin\java.exe")) {
        Write-Host "[FAIL] Khong tim thay java.exe trong JAVA_HOME" -ForegroundColor Red
        $ok = $false
    }
    if (-not (Test-Path "D:\Metadata\hadoop\bin\winutils.exe")) {
        Write-Host "[FAIL] Thieu winutils.exe - Bronze se bi rong" -ForegroundColor Red
        $ok = $false
    }
    if (-not (Test-Path ".\.venv311\Scripts\python3.exe")) {
        Write-Host "[WARN] Thieu python3.exe trong venv - Spark co the loi 'Cannot run program python3'" -ForegroundColor Yellow
    }
    foreach ($f in @("Data\raw\2019-Oct.csv", "Data\raw\2019-Nov.csv")) {
        if (-not (Test-Path $f)) {
            Write-Host "[FAIL] Thieu file du lieu: $f" -ForegroundColor Red
            $ok = $false
        }
    }
    if (-not $ok) { exit 1 }
    Write-Host "[OK] Moi truong san sang." -ForegroundColor Green
}

# --- Danh sach cac buoc ---
$STEPS = @(
    @{ N = 1; Name = "collect";      Script = "Data_collect.py";      Log = "collect_run.log" },
    @{ N = 2; Name = "cleaning";     Script = "Data_cleaning.py";     Log = "cleaning_run.log" },
    @{ N = 3; Name = "cdc";          Script = "Data_CDC.py";          Log = "cdc_run.log" },
    @{ N = 4; Name = "silver";       Script = "Data_Silver.py";       Log = "silver_run.log" },
    @{ N = 5; Name = "verify";       Script = "Data_Verify.py";       Log = "verify_run.log" },
    @{ N = 6; Name = "gold";         Script = "Data_Gold.py";         Log = "gold_run.log" },
    @{ N = 7; Name = "dashboard";    Script = "Data_Dashboard.py";    Log = "dashboard_run.log" },
    @{ N = 8; Name = "architecture"; Script = "Data_Architecture.py"; Log = "architecture_run.log" }
)

New-Item -ItemType Directory -Force -Path $LOGDIR | Out-Null
Test-Env

foreach ($s in $STEPS) {
    $run = $true
    if ($Only -ne "") {
        $run = ($s.Name -eq $Only.ToLower())
    } elseif ($s.N -lt $From) {
        $run = $false
    }
    if (-not $run) { continue }

    Write-Host ""
    Write-Host ("=" * 62) -ForegroundColor Cyan
    Write-Host ("BUOC {0}/8  ->  {1}  ({2})" -f $s.N, $s.Name, $s.Script) -ForegroundColor Cyan
    Write-Host ("=" * 62) -ForegroundColor Cyan

    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    & $PY $s.Script 2>&1 | Tee-Object "$LOGDIR\$($s.Log)"
    $code = $LASTEXITCODE
    $sw.Stop()

    if ($code -ne 0) {
        Write-Host ("[FAIL] {0} that bai voi exit code {1} sau {2:N1}s" -f $s.Script, $code, $sw.Elapsed.TotalSeconds) -ForegroundColor Red
        Write-Host ("       Log: {0}\{1}" -f $LOGDIR, $s.Log) -ForegroundColor Red
        Write-Host "       DUNG LAI tu buoc nay - cac buoc sau se doc du lieu cu." -ForegroundColor Yellow
        exit $code
    }
    Write-Host ("[OK] {0} xong trong {1:N1}s" -f $s.Script, $sw.Elapsed.TotalSeconds) -ForegroundColor Green
}

Write-Host ""
Write-Host ("=" * 62) -ForegroundColor Green
Write-Host "HOAN THANH PIPELINE" -ForegroundColor Green
Write-Host ("=" * 62) -ForegroundColor Green
Write-Host "Ket qua:"
Write-Host "  Data/bronze/    - Bronze Parquet + CDC"
Write-Host "  Data/silver/    - fact_event + 3 bang chieu"
Write-Host "  Data/gold/      - 8 bang KPI"
Write-Host "  Data/dashboard/ - 9 PNG + 9 CSV"
Write-Host "Log: $LOGDIR\"
