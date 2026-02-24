"""
run_audit.py — Unified pipeline runner
=======================================
Runs all three stages in order:
    1. Pricebook Cleaner
    2. SAP Data Cleaner  
    3. Audit Engine

SETUP:
    1. Set all file paths in CONFIGURATION below
    2. Run: python run_audit.py

That's it. Output saved to OUTPUT_FILE path.
"""

import sys
import os
import time
from pathlib import Path

# ============================================================
# CONFIGURATION — edit these paths each month
# ============================================================
PRICEBOOK_FILE  = r"C:\Users\nbrown2\Downloads\Master VetSoft Price Book 2026 - Updated for January 2026 (2).xlsx"
SAP_FILE        = r"C:\Users\nbrown2\OneDrive - IDEXX\sap export jan.xlsx"
OUTPUT_FILE     = r"C:\Users\nbrown2\Downloads\billing_audit.xlsx"

# Intermediate files — saved alongside output, no need to change
_out_dir        = str(Path(OUTPUT_FILE).parent)
PRICEBOOK_CLEAN = str(Path(_out_dir) / 'pricebook_clean.xlsx')
SAP_CLEAN       = str(Path(_out_dir) / 'sap_clean.xlsx')
# ============================================================

SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))


def banner(title, width=65):
    print("\n" + "=" * width)
    print(f"  {title}")
    print("=" * width)


def run_stage(stage_num, stage_name, module_file, run_func_patcher):
    """Run a single pipeline stage, timing it and catching errors."""
    banner(f"Stage {stage_num}: {stage_name}")
    start = time.time()
    try:
        run_func_patcher()
        elapsed = time.time() - start
        print(f"\n  Stage {stage_num} complete ({elapsed:.1f}s)")
        return True
    except SystemExit as e:
        # Scripts call sys.exit(1) on validation failure
        print(f"\n  Stage {stage_num} FAILED — pipeline stopped.")
        print(f"  Fix the issue above and re-run: python run_audit.py")
        return False
    except Exception as e:
        elapsed = time.time() - start
        print(f"\n  Stage {stage_num} FAILED after {elapsed:.1f}s")
        print(f"  {type(e).__name__}: {e}")
        print(f"\n  Fix the issue above and re-run: python run_audit.py")
        return False


def main():
    total_start = time.time()

    banner("IDEXX Billing Audit Pipeline", width=65)
    print(f"  Pricebook:  {PRICEBOOK_FILE}")
    print(f"  SAP export: {SAP_FILE}")
    print(f"  Output:     {OUTPUT_FILE}")

    # ---- STAGE 1: Pricebook Cleaner ----
    def run_pricebook():
        sys.path.insert(0, SCRIPTS_DIR)
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "pricebook_cleaner",
            os.path.join(SCRIPTS_DIR, "pricebook_cleaner.py")
        )
        mod = importlib.util.module_from_spec(spec)
        # Override the file path config before running
        spec.loader.exec_module(mod)
        mod.run(PRICEBOOK_FILE)
        # Move output to shared location if different from pricebook folder
        src = Path(PRICEBOOK_FILE).parent / 'pricebook_clean.xlsx'
        dst = Path(PRICEBOOK_CLEAN)
        if src != dst:
            import shutil
            shutil.copy2(src, dst)

    if not run_stage(1, "Pricebook Cleaner", "pricebook_cleaner.py", run_pricebook):
        sys.exit(1)

    # ---- STAGE 2: SAP Cleaner ----
    def run_sap():
        sys.path.insert(0, SCRIPTS_DIR)
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "sap_cleaner",
            os.path.join(SCRIPTS_DIR, "sap_cleaner.py")
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        mod.run(SAP_FILE)
        src = Path(SAP_FILE).parent / 'sap_clean.xlsx'
        dst = Path(SAP_CLEAN)
        if src != dst:
            import shutil
            shutil.copy2(src, dst)

    if not run_stage(2, "SAP Data Cleaner", "sap_cleaner.py", run_sap):
        sys.exit(1)

    # ---- STAGE 3: Audit Engine ----
    def run_audit():
        sys.path.insert(0, SCRIPTS_DIR)
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "audit_engine",
            os.path.join(SCRIPTS_DIR, "audit_engine.py")
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        # Override paths in the module before running
        mod.PRICEBOOK_CLEAN = PRICEBOOK_CLEAN
        mod.SAP_CLEAN       = SAP_CLEAN
        mod.OUTPUT_FILE     = OUTPUT_FILE
        mod.run()

    if not run_stage(3, "Audit Engine", "audit_engine.py", run_audit):
        sys.exit(1)

    # ---- DONE ----
    total_elapsed = time.time() - total_start
    banner("PIPELINE COMPLETE", width=65)
    print(f"  Total time: {total_elapsed:.1f}s")
    print(f"  Output:     {OUTPUT_FILE}")
    print(f"  Run log:    {Path(OUTPUT_FILE).parent / 'run_log.txt'}")
    print("=" * 65)


if __name__ == '__main__':
    main()
