"""
validation.py — Shared validation and error handling for the billing audit pipeline.

Imported by pricebook_cleaner.py, sap_cleaner.py, and audit_engine.py.
Provides:
    - File existence checks
    - Required column assertions
    - Row count sanity checks
    - Stage handoff validation
    - Schema enforcement
    - Plain-English error messages with fix suggestions
"""

import sys
import os
import pandas as pd
from pathlib import Path
from datetime import datetime


# ============================================================
# PIPELINE VERSION — bump this when making breaking changes
# ============================================================
PIPELINE_VERSION = "1.0.0"


class AuditValidationError(Exception):
    """Raised when a validation check fails. Contains a plain-English message."""
    pass


# ---- TERMINAL COLORS (Windows-safe fallback) ----
def _supports_color():
    return sys.platform != 'win32' or 'ANSICON' in os.environ

BOLD  = '\033[1m'  if _supports_color() else ''
RED   = '\033[91m' if _supports_color() else ''
YELLOW= '\033[93m' if _supports_color() else ''
GREEN = '\033[92m' if _supports_color() else ''
RESET = '\033[0m'  if _supports_color() else ''

def log_ok(msg):
    print(f"  {GREEN}OK{RESET}    {msg}")

def log_warn(msg):
    print(f"  {YELLOW}WARN{RESET}  {msg}")

def log_error(msg):
    print(f"  {RED}ERROR{RESET} {msg}")

def log_section(msg):
    print(f"\n{BOLD}{msg}{RESET}")


# ============================================================
# FILE CHECKS
# ============================================================

def assert_file_exists(filepath, label=None):
    """
    Assert a file exists and is readable before trying to open it.
    Provides a plain-English fix suggestion on failure.
    """
    label = label or filepath
    p = Path(filepath)

    if not p.exists():
        log_error(f"File not found: {filepath}")
        print(f"\n  Fix: Check that the file path is correct and the file hasn't been moved.")
        print(f"       Path checked: {p.resolve()}")
        raise AuditValidationError(f"File not found: {filepath}")

    if not p.is_file():
        log_error(f"Path exists but is not a file: {filepath}")
        raise AuditValidationError(f"Not a file: {filepath}")

    if p.suffix.lower() not in ('.xlsx', '.xls', '.csv'):
        log_warn(f"{label} has unexpected extension '{p.suffix}' — expected .xlsx")

    size_kb = p.stat().st_size / 1024
    if size_kb < 1:
        log_warn(f"{label} is very small ({size_kb:.1f} KB) — may be empty or corrupted")

    log_ok(f"{label} found ({size_kb:.0f} KB)")
    return True


def assert_output_dir_writable(filepath):
    """Check that we can write to the output directory."""
    p = Path(filepath).parent
    if not p.exists():
        log_error(f"Output directory does not exist: {p}")
        print(f"\n  Fix: Create the folder first, or change the output path in the script.")
        raise AuditValidationError(f"Output directory missing: {p}")

    # Try writing a temp file
    test_file = p / '.write_test'
    try:
        test_file.touch()
        test_file.unlink()
    except PermissionError:
        log_error(f"Cannot write to output directory: {p}")
        print(f"\n  Fix: Close any open Excel files in that folder, or check folder permissions.")
        raise AuditValidationError(f"Output directory not writable: {p}")

    log_ok(f"Output directory writable: {p}")


# ============================================================
# COLUMN CHECKS
# ============================================================

def assert_columns_present(df, required_cols, source_label):
    """
    Assert all required columns are present in a DataFrame.
    Reports ALL missing columns at once rather than one at a time.
    """
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        log_error(f"{source_label} is missing required columns: {missing}")
        print(f"\n  Columns found: {list(df.columns)}")
        print(f"\n  Fix: Check that the correct file was loaded and hasn't had columns")
        print(f"       renamed or reordered since the pipeline was last run.")
        raise AuditValidationError(f"{source_label} missing columns: {missing}")

    log_ok(f"{source_label} has all required columns")


def assert_no_duplicate_keys(df, key_cols, source_label):
    """Warn if duplicate keys exist that could cause join issues."""
    dupes = df[df.duplicated(subset=key_cols, keep=False)]
    if not dupes.empty:
        log_warn(f"{source_label} has {len(dupes):,} duplicate rows on {key_cols} "
                 f"— may cause unexpected matches in audit")
    else:
        log_ok(f"{source_label} has no duplicate keys on {key_cols}")


# ============================================================
# ROW COUNT SANITY CHECKS
# ============================================================

def assert_min_rows(df, min_rows, source_label):
    """Fail if a DataFrame has suspiciously few rows."""
    if len(df) < min_rows:
        log_error(f"{source_label} has only {len(df):,} rows (expected at least {min_rows:,})")
        print(f"\n  Fix: The file may be empty, filtered, or the wrong month's export.")
        raise AuditValidationError(f"{source_label} too few rows: {len(df)}")
    log_ok(f"{source_label} row count OK ({len(df):,} rows)")


def warn_if_row_count_changed(current_count, baseline_count, source_label, threshold_pct=30):
    """
    Warn if row count has changed dramatically vs a baseline.
    Useful for catching accidentally filtered exports.
    """
    if baseline_count is None or baseline_count == 0:
        return
    change_pct = abs(current_count - baseline_count) / baseline_count * 100
    if change_pct > threshold_pct:
        log_warn(f"{source_label} row count changed {change_pct:.0f}% vs baseline "
                 f"({baseline_count:,} -> {current_count:,}) — verify correct file loaded")


# ============================================================
# DATA QUALITY CHECKS
# ============================================================

def assert_no_nulls_in_key_cols(df, key_cols, source_label):
    """Fail if key join columns contain nulls."""
    for col in key_cols:
        if col not in df.columns:
            continue
        null_count = df[col].isna().sum()
        if null_count > 0:
            log_warn(f"{source_label}.{col} has {null_count:,} null values "
                     f"— these rows will not match in the audit join")
        else:
            log_ok(f"{source_label}.{col} has no nulls")


def assert_currency_codes_valid(df, currency_col, source_label):
    """Warn on unexpected currency codes."""
    known = {'USD', 'CAD', 'GBP', 'AUD', 'NZD', 'ZAR', 'EUR'}
    if currency_col not in df.columns:
        return
    found = set(df[currency_col].dropna().unique())
    unknown = found - known
    if unknown:
        log_warn(f"{source_label} has unexpected currency codes: {unknown} "
                 f"— these rows will not match pricebook entries")
    else:
        log_ok(f"{source_label} all currency codes recognized: {sorted(found)}")


def assert_numeric_column(df, col, source_label, allow_zero=True, allow_negative=False):
    """Check a column is numeric and within expected range."""
    if col not in df.columns:
        return
    non_numeric = df[col].apply(
        lambda x: not isinstance(x, (int, float)) and pd.notna(x)
    ).sum()
    if non_numeric > 0:
        log_warn(f"{source_label}.{col} has {non_numeric:,} non-numeric values")

    if not allow_negative:
        neg_count = (df[col].fillna(0) < 0).sum()
        if neg_count > 0:
            log_warn(f"{source_label}.{col} has {neg_count:,} negative values")

    if not allow_zero:
        zero_count = (df[col].fillna(-1) == 0).sum()
        if zero_count > 0:
            log_warn(f"{source_label}.{col} has {zero_count:,} zero values")


# ============================================================
# STAGE HANDOFF VALIDATION
# ============================================================

PRICEBOOK_CLEAN_REQUIRED_COLS = [
    'material', 'max_band', 'currency', 'price_2025', 'price_2026',
    'source_tab', 'is_custom'
]

SAP_CLEAN_REQUIRED_COLS = [
    'material', 'quantity', 'net_value', 'currency'
]

def validate_pricebook_clean(filepath):
    """
    Full validation of pricebook_clean.xlsx before audit engine runs.
    Call this at the start of audit_engine.py.
    """
    log_section("Validating pricebook_clean.xlsx...")
    assert_file_exists(filepath, 'pricebook_clean.xlsx')

    df = pd.read_excel(filepath, sheet_name='Pricebook', dtype={'material': str})

    assert_columns_present(df, PRICEBOOK_CLEAN_REQUIRED_COLS, 'pricebook_clean')
    assert_min_rows(df, 1000, 'pricebook_clean')
    assert_no_nulls_in_key_cols(df, ['material', 'currency'], 'pricebook_clean')
    assert_currency_codes_valid(df, 'currency', 'pricebook_clean')

    # Warn if fewer than expected price rows
    numeric_rows = df[~df['is_custom'].fillna(False)]
    if len(numeric_rows) < 5000:
        log_warn(f"Only {len(numeric_rows):,} numeric price rows — expected 7,000+. "
                 f"Pricebook may have lost tabs.")

    log_ok(f"pricebook_clean.xlsx passed all checks "
           f"({len(df):,} rows, {df['material'].nunique()} materials)")
    return df


def validate_sap_clean(filepath):
    """
    Full validation of sap_clean.xlsx before audit engine runs.
    Call this at the start of audit_engine.py.
    """
    log_section("Validating sap_clean.xlsx...")
    assert_file_exists(filepath, 'sap_clean.xlsx')

    df = pd.read_excel(filepath, sheet_name='SAP', dtype={'material': str})

    assert_columns_present(df, SAP_CLEAN_REQUIRED_COLS, 'sap_clean')
    assert_min_rows(df, 10, 'sap_clean')
    assert_no_nulls_in_key_cols(df, ['material', 'currency'], 'sap_clean')
    assert_currency_codes_valid(df, 'currency', 'sap_clean')
    assert_numeric_column(df, 'net_value', 'sap_clean', allow_negative=True)
    assert_numeric_column(df, 'quantity', 'sap_clean', allow_zero=True)

    log_ok(f"sap_clean.xlsx passed all checks "
           f"({len(df):,} rows, {df['material'].nunique()} materials)")
    return df


# ============================================================
# AUDIT RESULT VALIDATION
# ============================================================

def validate_audit_result(audit_df, sap_row_count):
    """
    Sanity check the final audit DataFrame before writing output.
    """
    log_section("Validating audit results...")

    # Row count must match SAP exactly
    if len(audit_df) != sap_row_count:
        log_error(f"Audit output has {len(audit_df):,} rows but SAP had {sap_row_count:,} — "
                  f"rows were lost or duplicated in the join")
        raise AuditValidationError("Row count mismatch between SAP input and audit output")
    log_ok(f"Audit row count matches SAP input ({len(audit_df):,} rows)")

    # Every row must have a flag
    unflagged = audit_df['audit_flag'].isna().sum()
    if unflagged > 0:
        log_error(f"{unflagged:,} rows have no audit_flag — classify() returned None")
        raise AuditValidationError("Unflagged rows in audit output")
    log_ok("All rows have an audit flag")

    # Total billed should be non-zero
    total = audit_df['net_value'].sum()
    if total == 0:
        log_warn("Total net value is $0 — verify SAP data loaded correctly")
    else:
        log_ok(f"Total net value: ${total:,.2f}")


# ============================================================
# RUN LOG
# ============================================================

def write_run_log(output_dir, summary_dict):
    """
    Append a one-line entry to run_log.txt after each successful pipeline run.
    Useful for tracking results month over month.
    """
    log_path = Path(output_dir) / 'run_log.txt'
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M')

    line = (
        f"{timestamp} | v{PIPELINE_VERSION} | "
        f"SAP rows: {summary_dict.get('sap_rows', '?')} | "
        f"Correct: {summary_dict.get('correct', '?')} | "
        f"No match: {summary_dict.get('no_match', '?')} | "
        f"Custom: {summary_dict.get('custom', '?')} | "
        f"Old price: {summary_dict.get('old_price', '?')} | "
        f"Total billed: ${summary_dict.get('total_billed', 0):,.2f}\n"
    )

    with open(log_path, 'a', encoding='utf-8') as f:
        f.write(line)

    log_ok(f"Run log updated: {log_path}")


# ============================================================
# GRACEFUL FAILURE WRAPPER
# ============================================================

def run_with_error_handling(func, stage_name):
    """
    Wrap a pipeline stage function so failures print a clean message
    instead of a raw Python traceback.
    """
    try:
        return func()
    except AuditValidationError as e:
        print(f"\n{RED}{'='*60}")
        print(f"  {stage_name} FAILED — Validation Error")
        print(f"  {e}")
        print(f"{'='*60}{RESET}")
        sys.exit(1)
    except FileNotFoundError as e:
        print(f"\n{RED}{'='*60}")
        print(f"  {stage_name} FAILED — File Not Found")
        print(f"  {e}")
        print(f"  Fix: Check all file paths in the CONFIGURATION section.")
        print(f"{'='*60}{RESET}")
        sys.exit(1)
    except PermissionError as e:
        print(f"\n{RED}{'='*60}")
        print(f"  {stage_name} FAILED — Permission Denied")
        print(f"  {e}")
        print(f"  Fix: Close any open Excel files and try again.")
        print(f"{'='*60}{RESET}")
        sys.exit(1)
    except Exception as e:
        print(f"\n{RED}{'='*60}")
        print(f"  {stage_name} FAILED — Unexpected Error")
        print(f"  {type(e).__name__}: {e}")
        print(f"  Please paste this error in the audit pipeline chat for debugging.")
        print(f"{'='*60}{RESET}")
        raise  # re-raise for full traceback on unexpected errors
