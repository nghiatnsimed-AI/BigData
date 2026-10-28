#!/usr/bin/env python
# ===============================================================
# RUN_ALL.PY — Chạy toàn bộ pipeline Big Data theo đúng thứ tự
# ===============================================================
# Dùng cho ai không muốn dùng PowerShell (hoặc chạy trên Linux/Mac).
#
#   python run_all.py              → chạy cả 8 bước
#   python run_all.py --from 4     → chạy lại từ bước 4 (Data_Silver) trở đi
#   python run_all.py --only gold  → chạy đúng 1 bước theo tên
#
# Tên bước: collect, cleaning, cdc, silver, verify, gold, dashboard, architecture
# ===============================================================
import os
import subprocess
import sys
import time

BASE = os.path.dirname(os.path.abspath(__file__))
os.chdir(BASE)

# --- Dùng đúng python của venv (không dùng python hệ thống) ---
PY = sys.executable

STEPS = [
    (1, "collect",      "Data_collect.py",      "collect_run.log"),
    (2, "cleaning",     "Data_cleaning.py",     "cleaning_run.log"),
    (3, "cdc",          "Data_CDC.py",          "cdc_run.log"),
    (4, "silver",       "Data_Silver.py",       "silver_run.log"),
    (5, "verify",       "Data_Verify.py",       "verify_run.log"),
    (6, "gold",         "Data_Gold.py",         "gold_run.log"),
    (7, "dashboard",    "Data_Dashboard.py",    "dashboard_run.log"),
    (8, "architecture", "Data_Architecture.py", "architecture_run.log"),
]

LOGDIR = os.path.join("Data", "logs")


def check_env():
    """Kiểm tra môi trường trước khi chạy. Trả về True nếu OK."""
    ok = True
    java_home = os.environ.get("JAVA_HOME", "")
    if not java_home:
        print("[FAIL] JAVA_HOME chua duoc dat", file=sys.stderr)
        ok = False
    elif not os.path.exists(os.path.join(java_home, "bin", "java.exe")):
        print(f"[FAIL] Khong tim thay java.exe trong JAVA_HOME ({java_home})", file=sys.stderr)
        ok = False
    if not os.path.exists(r"D:\Metadata\hadoop\bin\winutils.exe"):
        print("[FAIL] Thieu winutils.exe - Bronze se bi rong", file=sys.stderr)
        ok = False
    for f in (r"Data\raw\2019-Oct.csv", r"Data\raw\2019-Nov.csv"):
        if not os.path.exists(f):
            print(f"[FAIL] Thieu file du lieu: {f}", file=sys.stderr)
            ok = False
    if ok:
        print("[OK] Moi truong san sang.")
    return ok


def main():
    start_from = 1
    only = ""
    args = sys.argv[1:]
    for i, a in enumerate(args):
        if a in ("--from", "-f") and i + 1 < len(args):
            start_from = int(args[i + 1])
        elif a in ("--only", "-o") and i + 1 < len(args):
            only = args[i + 1].lower()

    os.makedirs(LOGDIR, exist_ok=True)
    if not check_env():
        sys.exit(1)

    for n, name, script, log in STEPS:
        if only:
            if name != only:
                continue
        elif n < start_from:
            continue

        print("\n" + "=" * 62)
        print(f"BUOC {n}/8  ->  {name}  ({script})")
        print("=" * 62, flush=True)

        log_path = os.path.join(LOGDIR, log)
        t0 = time.time()
        # Ghi log ra file VÀ hiện ra màn hình cùng lúc
        with open(log_path, "w", encoding="utf-8", errors="replace") as fh:
            proc = subprocess.Popen(
                [PY, script],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
            )
            for line in proc.stdout:
                print(line, end="")
                fh.write(line)
            code = proc.wait()
        dt = time.time() - t0

        if code != 0:
            print(f"[FAIL] {script} that bai voi exit code {code} sau {dt:.1f}s")
            print(f"       Log: {log_path}")
            print("       DUNG LAI tu buoc nay - cac buoc sau se doc du lieu cu.")
            sys.exit(code)
        print(f"[OK] {script} xong trong {dt:.1f}s")

    print("\n" + "=" * 62)
    print("HOAN THANH PIPELINE")
    print("=" * 62)
    print("Ket qua:")
    print("  Data/bronze/    - Bronze Parquet + CDC")
    print("  Data/silver/    - fact_event + 3 bang chieu")
    print("  Data/gold/      - 8 bang KPI")
    print("  Data/dashboard/ - 9 PNG + 9 CSV")
    print(f"Log: {LOGDIR}/")


if __name__ == "__main__":
    main()
