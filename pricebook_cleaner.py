"""
TOOL 1 OF 3 — Pricebook Cleaner
================================
Reads every tab of the IDEXX pricebook and outputs one flat,
queryable Excel file where each row is:

    material | max_band | currency | price_2025 | price_2026 | source_tab | is_custom

SETUP:
    pip install pandas openpyxl

USAGE:
    1. Set PRICEBOOK_FILE path below
    2. Run: python pricebook_cleaner.py
    3. Output saved to same folder as pricebook
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
# CONFIGURATION
# ============================================================
PRICEBOOK_FILE = r"C:\Users\nbrown2\Downloads\Master VetSoft Price Book 2026 - Updated for January 2026 (2).xlsx"
# ============================================================

CURRENCIES = ['USD', 'CAD', 'GBP', 'AUD', 'NZD', 'ZAR', 'EUR']

def detect_currency(col_name):
    col_upper = str(col_name).upper()
    col_str = str(col_name)
    for code in CURRENCIES:
        if code in col_upper:
            return code
    if 'CANADA' in col_upper:
        return 'CAD'
    if col_upper.startswith('US ') or ' US ' in col_upper:
        return 'USD'
    if '£' in col_str or '\xa3' in col_str:
        return 'GBP'
    if '€' in col_str or '\u20ac' in col_str:
        return 'EUR'
    if 'NZ$' in col_str:
        return 'NZD'
    if 'A$' in col_str or 'AU$' in col_str:
        return 'AUD'
    if 'AUS ' in col_upper or col_upper.startswith('AUS	') or 'AUS LIST' in col_upper:
        return 'AUD'
    if 'CA$' in col_str or 'CAN$' in col_str:
        return 'CAD'
    return None

def detect_year(col_name):
    col_str = str(col_name)
    if '2026' in col_str:
        return '2026'
    if '2025' in col_str:
        return '2025'
    return None

def clean_number(val):
    """Convert string prices with any currency symbol to float."""
    if pd.isna(val):
        return None
    try:
        cleaned = str(val)
        for sym in ['$', '£', '€', '\xa3', '\u20ac']:
            cleaned = cleaned.replace(sym, '')
        cleaned = cleaned.strip()
        if cleaned.startswith('R') and len(cleaned) > 1 and cleaned[1].isdigit():
            cleaned = cleaned[1:]
        cleaned = cleaned.replace(',', '').replace(' ', '').strip()
        result = float(cleaned)
        return result if result > 0 else None
    except (ValueError, TypeError):
        return None

def is_custom_value(val):
    """Return True if the cell contains non-numeric custom pricing text."""
    if pd.isna(val):
        return False
    cleaned = str(val).strip().upper()
    return cleaned in ('CUSTOM', 'PRICING BASED ON CONTRACT', 'TBD', 'N/A') or            cleaned.startswith('PRICING BASED')

def find_part_number_col(columns):
    for col in columns:
        col_upper = str(col).upper()
        if 'IDEXX' in col_upper and 'PART' in col_upper:
            return col
        if col_upper in ('MATERIAL', 'PART NUMBER', 'PART NO', 'PART#'):
            return col
    return None

def find_max_band_col(columns):
    priority = [
        'max of user tier', 'max of tier', 'max of seats',
        'max tier', 'max seats', 'scale quantity', 'number of seats',
    ]
    for target in priority:
        for col in columns:
            if target in str(col).lower():
                return col
    return None

def find_min_band_col(columns):
    priority = [
        'min of user tier', 'min of tier', 'min of seats',
        'min tier', 'min seats',
    ]
    for target in priority:
        for col in columns:
            if target in str(col).lower():
                return col
    return None

def find_header_row(raw_df):
    for i, row in raw_df.iterrows():
        row_str = ' '.join(str(v).upper() for v in row.values if pd.notna(v))
        if 'IDEXX' in row_str or 'MATERIAL' in row_str or 'PART NUMBER' in row_str:
            return i
    return None

def process_tab(xl, sheet_name):
    raw = xl.parse(sheet_name, header=None)
    header_row = find_header_row(raw)
    if header_row is None:
        return [], "no header row found (no IDEXX/Material/Part Number column)"

    df = xl.parse(sheet_name, header=header_row)
    df.columns = [str(c).strip() for c in df.columns]
    df = df.dropna(how='all').reset_index(drop=True)

    part_col = find_part_number_col(df.columns)
    if part_col is None:
        return [], "could not identify part number column"

    max_col = find_max_band_col(df.columns)
    min_col = find_min_band_col(df.columns)

    price_cols = []
    for col in df.columns:
        currency = detect_currency(col)
        year = detect_year(col)
        if currency and year:
            price_cols.append((col, currency, year))

    if not price_cols:
        return [], "no price columns detected (looking for currency + year in header)"

    rows = []
    custom_materials = set()  # track materials with all-Custom pricing

    for _, row in df.iterrows():
        material = str(row.get(part_col, '')).strip()
        if not material or material.lower() in ('nan', 'none', ''):
            continue
        if any(kw in material.upper() for kw in ('IDEXX', 'PART', 'MATERIAL')):
            continue

        max_band = clean_number(row[max_col]) if max_col else None
        if max_band is None and min_col:
            max_band = clean_number(row.get(min_col))

        # Check if all price cells are "Custom" for this row
        row_has_custom = any(is_custom_value(row.get(col)) for col, _, _ in price_cols)
        row_has_numeric = False

        prices = {}
        for col, currency, year in price_cols:
            price = clean_number(row.get(col))
            if price is not None:
                prices.setdefault(currency, {})[year] = price
                row_has_numeric = True

        if row_has_numeric:
            for currency, year_prices in prices.items():
                rows.append({
                    'material':   material,
                    'max_band':   max_band,
                    'currency':   currency,
                    'price_2025': year_prices.get('2025'),
                    'price_2026': year_prices.get('2026'),
                    'source_tab': sheet_name,
                    'is_custom':  False,
                })
        elif row_has_custom:
            # Record this material as custom-priced with null prices
            # One row per currency found in headers so audit engine can detect it
            custom_materials.add(material)
            for _, currency, _ in price_cols:
                rows.append({
                    'material':   material,
                    'max_band':   None,
                    'currency':   currency,
                    'price_2025': None,
                    'price_2026': None,
                    'source_tab': sheet_name,
                    'is_custom':  True,
                })

    # Deduplicate custom rows (same material+currency may appear many times)
    if rows:
        df_rows = pd.DataFrame(rows)
        # For custom rows keep one per material+currency; numeric rows keep all (tiers)
        numeric = df_rows[~df_rows['is_custom']]
        custom  = df_rows[df_rows['is_custom']].drop_duplicates(subset=['material', 'currency'])
        rows = pd.concat([numeric, custom], ignore_index=True).to_dict('records')

    if not rows:
        return [], "tab parsed but zero data rows extracted"

    if custom_materials:
        print(f"         ^ {len(custom_materials)} custom-priced material(s) recorded: "
              f"{', '.join(sorted(custom_materials))}")

    return rows, None


def run(pricebook_path):
    print("=" * 60)
    print(f"Pricebook Cleaner  (pipeline v{PIPELINE_VERSION})")
    print("=" * 60)
    if VALIDATION_AVAILABLE:
        log_section("Validating inputs...")
        assert_file_exists(pricebook_path, 'pricebook')
        output_path = Path(pricebook_path).parent / 'pricebook_clean.xlsx'
        assert_output_dir_writable(str(output_path))
    print(f"\nReading: {pricebook_path}")

    xl = pd.ExcelFile(pricebook_path)
    print(f"Found {len(xl.sheet_names)} tabs\n")

    all_rows = []
    skipped = []

    for sheet_name in xl.sheet_names:
        rows, skip_reason = process_tab(xl, sheet_name)
        if skip_reason:
            skipped.append((sheet_name, skip_reason))
            print(f"  SKIP  {sheet_name!r:40s}  -> {skip_reason}")
        else:
            all_rows.extend(rows)
            row_count = len([r for r in rows if not r['is_custom']])
            custom_count = len([r for r in rows if r['is_custom']])
            suffix = f" + {custom_count} custom" if custom_count else ""
            print(f"  OK    {sheet_name!r:40s}  -> {row_count:,} price rows{suffix}")

    print(f"\n{'─'*60}")
    print(f"Tabs processed:  {len(xl.sheet_names) - len(skipped)}")
    print(f"Tabs skipped:    {len(skipped)}")

    if not all_rows:
        if VALIDATION_AVAILABLE:
            log_error("No data extracted from pricebook.")
            print("\n  Fix: Check PRICEBOOK_FILE path and that the file has IDEXX Part Number columns.")
        else:
            print("\nERROR: No data extracted.")
        return

    df_out = pd.DataFrame(all_rows)
    numeric_rows = df_out[~df_out['is_custom']]
    custom_rows  = df_out[df_out['is_custom']]

    print(f"Total price rows:  {len(numeric_rows):,}")
    print(f"Total custom rows: {len(custom_rows):,} "
          f"({custom_rows['material'].nunique()} materials)")

    df_out['max_band'] = pd.to_numeric(df_out['max_band'], errors='coerce')
    df_out = df_out.sort_values(['material', 'currency', 'max_band']).reset_index(drop=True)

    print(f"\nSummary:")
    print(f"  Unique materials:      {df_out['material'].nunique():,}")
    print(f"  Currencies:            {', '.join(sorted(df_out['currency'].unique()))}")
    print(f"  Has 2025 price:        {df_out['price_2025'].notna().sum():,} rows")
    print(f"  Has 2026 price:        {df_out['price_2026'].notna().sum():,} rows")
    print(f"  Has tier bands:        {df_out['max_band'].notna().sum():,} rows")
    print(f"  Fixed price (no band): {df_out['max_band'].isna().sum():,} rows")

    output_path = Path(pricebook_path).parent / 'pricebook_clean.xlsx'
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        df_out.to_excel(writer, sheet_name='Pricebook', index=False)
        ws = writer.sheets['Pricebook']
        for col in ws.columns:
            max_len = max(len(str(c.value)) if c.value is not None else 0 for c in col)
            ws.column_dimensions[col[0].column_letter].width = min(max_len + 2, 40)
        from openpyxl.styles import Font, PatternFill
        for cell in ws[1]:
            cell.font = Font(bold=True, color='FFFFFF')
            cell.fill = PatternFill('solid', start_color='1F4E79')

    print(f"\nOutput saved: {output_path}")
    print("=" * 60)


if __name__ == '__main__':
    run(PRICEBOOK_FILE)
