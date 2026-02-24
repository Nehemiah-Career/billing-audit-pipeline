"""
TOOL 2 OF 3 — SAP Data Cleaner
================================
Reads your SAP billing export and outputs a clean, standardized
Excel file ready to join against pricebook_clean.xlsx.

Cleans:
    - Net Value: strips commas, converts to float
    - Order Quantity: converts to numeric, keeps zeros (one-time fees)
    - Material: strips whitespace
    - Currency: uppercases and strips whitespace
    - Drops rows missing Material or Net Value only

SETUP:
    pip install pandas openpyxl

USAGE:
    1. Set SAP_FILE path below
    2. Run: python sap_cleaner.py
    3. Output saved to same folder as SAP file
"""

import pandas as pd
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    from validation import (
        assert_file_exists, assert_output_dir_writable,
        assert_min_rows, run_with_error_handling,
        log_section, log_ok, log_warn, log_error, PIPELINE_VERSION
    )
    VALIDATION_AVAILABLE = True
except ImportError:
    VALIDATION_AVAILABLE = False
    def log_section(m): print(f"\n{m}")
    def log_ok(m): print(f"  OK    {m}")
    def log_warn(m): print(f"  WARN  {m}")
    def log_error(m): print(f"  ERROR {m}")
    PIPELINE_VERSION = "unknown"


# ============================================================
# CONFIGURATION — set your SAP export path here
# ============================================================
SAP_FILE = r"C:\path\to\your\sap_export.xlsx"
# ============================================================


# Required columns — matched against actual SAP header names
COL_PATTERNS = {
    'material':  ['MATERIAL'],
    'quantity':  ['ORDER QUANT', 'ORDER QTY', 'ORDERQUAN', 'QUANTITY'],
    'net_value': ['NET VALUE', 'NETVALUE', 'NET VAL'],
    'currency':  ['CURR'],
}

# Context columns — carried through if found, not required
CONTEXT_PATTERNS = {
    'sales_org':     ['SORG', 'S ORG', 'SALES ORG'],
    'created_on':    ['CREATED ON', 'CREATEDON'],
    'order_number':  ['ORDER#', 'ORDER #', 'ORDER NUMBER'],
    'ship_to':       ['SHIP-TO', 'SHIP TO', 'SHIPTO'],
    'customer_name': ['NAME 1', 'NAME1', 'CUSTOMER NAME'],
    'address':       ['ADDRESS'],
    'status':        ['ST.', 'STATUS'],
    'sold_to':       ['SOLD TO', 'SOLDTO', 'SOLD-TO'],
    'description':   ['DESCRIPTION', 'SAP DESC', 'DESC'],
    'cost_group':    ['CGP', 'COST GROUP'],
}


def clean_number(val):
    if pd.isna(val):
        return None
    try:
        cleaned = str(val).replace('$', '').replace(',', '').replace(' ', '').strip()
        return float(cleaned)
    except (ValueError, TypeError):
        return None


def find_header_row(raw_df):
    for i, row in raw_df.head(20).iterrows():
        row_str = ' '.join(str(v).upper() for v in row.values if pd.notna(v))
        if 'MATERIAL' in row_str and 'CURR' in row_str:
            return i
    return 0


def match_column(columns, patterns):
    for col in columns:
        col_upper = str(col).upper().replace('.', '').replace('_', ' ')
        for pattern in patterns:
            if pattern in col_upper:
                return col
    return None


def run(sap_path):
    print("=" * 60)
    print(f"SAP Data Cleaner  (pipeline v{PIPELINE_VERSION})")
    print("=" * 60)
    if VALIDATION_AVAILABLE:
        log_section("Validating inputs...")
        assert_file_exists(sap_path, 'SAP export')
        output_path = Path(sap_path).parent / 'sap_clean.xlsx'
        assert_output_dir_writable(str(output_path))
    print(f"\nReading: {sap_path}")

    raw = pd.read_excel(sap_path, header=None)
    header_row = find_header_row(raw)
    if header_row > 0:
        print(f"  Header found at row {header_row + 1} (skipping {header_row} junk rows)")

    df = pd.read_excel(sap_path, header=header_row)
    df.columns = [str(c).strip() for c in df.columns]
    df = df.dropna(how='all').reset_index(drop=True)

    print(f"  Raw rows loaded: {len(df):,}")
    print(f"  Columns found:   {list(df.columns)}")

    col_map = {}
    for field, patterns in COL_PATTERNS.items():
        col = match_column(df.columns, patterns)
        if col:
            col_map[field] = col

    missing = [f for f in COL_PATTERNS if f not in col_map]
    if missing:
        if VALIDATION_AVAILABLE:
            log_error(f"Could not find required columns: {missing}")
            print(f"\n  Columns found in file: {list(df.columns)}")
            print(f"  Fix: Check that the SAP export contains Material, Order Quantity, Net Value, Curr.")
        else:
            print(f"\nERROR: Could not find columns for: {missing}")
            print(f"Available columns: {list(df.columns)}")
        return

    print(f"\n  Column mapping:")
    for field, col in col_map.items():
        print(f"    {field:<15} -> '{col}'")

    clean = pd.DataFrame()
    clean['material']  = df[col_map['material']].astype(str).str.strip()
    clean['quantity']  = df[col_map['quantity']].apply(clean_number).fillna(0)
    clean['net_value'] = df[col_map['net_value']].apply(clean_number)
    clean['currency']  = df[col_map['currency']].astype(str).str.strip().str.upper()

    for field, patterns in CONTEXT_PATTERNS.items():
        col = match_column(df.columns, patterns)
        if col:
            clean[field] = df[col].astype(str).str.strip()
            print(f"    {field:<15} -> '{col}' (context)")

    before = len(clean)
    clean = clean[clean['material'].notna() & (~clean['material'].isin(['nan', 'none', '']))]
    dropped_material = before - len(clean)

    after_material = len(clean)
    clean = clean[clean['net_value'].notna()]
    dropped_net = after_material - len(clean)

    zero_qty = (clean['quantity'] == 0).sum()

    print(f"\n  Rows dropped:")
    print(f"    Missing material:   {dropped_material:,}")
    print(f"    Missing net value:  {dropped_net:,}")
    print(f"    Clean rows:         {len(clean):,}")
    print(f"    (qty = 0):          {zero_qty:,}  <- one-time fees, no tier lookup needed")

    print(f"\n  Summary:")
    print(f"    Unique materials:  {clean['material'].nunique():,}")
    print(f"    Currencies:        {', '.join(sorted(clean['currency'].unique()))}")
    print(f"    Net value range:   ${clean['net_value'].min():,.2f} - ${clean['net_value'].max():,.2f}")
    tiered = clean[clean['quantity'] > 0]
    if not tiered.empty:
        print(f"    Quantity range:    {int(tiered['quantity'].min()):,} - {int(tiered['quantity'].max()):,}  (tiered rows only)")

    output_path = Path(sap_path).parent / 'sap_clean.xlsx'

    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        clean.to_excel(writer, sheet_name='SAP', index=False)
        ws = writer.sheets['SAP']
        for col in ws.columns:
            max_len = max(len(str(c.value)) if c.value is not None else 0 for c in col)
            ws.column_dimensions[col[0].column_letter].width = min(max_len + 2, 40)
        from openpyxl.styles import Font, PatternFill
        for cell in ws[1]:
            cell.font = Font(bold=True, color='FFFFFF')
            cell.fill = PatternFill('solid', start_color='1F4E79')

    print(f"\n  Output saved: {output_path}")
    print("=" * 60)


if __name__ == '__main__':
    run(SAP_FILE)
