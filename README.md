# IDEXX VetSoft Billing Audit Pipeline

A modular Python pipeline that automates monthly billing validation for veterinary software subscriptions. Replaces a manual Excel-based audit process, reducing review time from hours to minutes.

## What It Does

Each month, billing teams need to verify that SAP billing data matches contracted prices in the master pricebook. This pipeline automates that process across:

- **37+ product SKUs** with tiered, seat-based, and flat pricing models
- **7 currencies** (USD, CAD, GBP, AUD, NZD, ZAR, EUR)
- **1,100+ monthly billing rows** across international markets
- **Contract-priced** and custom-negotiated deals handled separately

The pipeline outputs a formatted Excel workbook with every billing row classified, color-coded, and ready for reviewer action — no manual lookups required.

## Architecture

```
run_audit.py          ← single entry point, runs all three stages
│
├── pricebook_cleaner.py   Stage 1: parse multi-tab pricebook → flat lookup table
├── sap_cleaner.py         Stage 2: standardize SAP export → clean billing rows  
├── audit_engine.py        Stage 3: join + classify every row
└── validation.py          shared error handling and schema checks
```

**Design principles:**
- Modular — each stage can be run and debugged independently
- Resilient — schema validation between every stage with plain-English error messages
- Transparent — every classification decision is traceable in the output
- Extensible — built for future Streamlit UI integration

## Audit Flags

| Flag | Meaning | Action |
|---|---|---|
| `CORRECT_2026`        | Billed at current 2026 price             | ✅ None |
| `PRICE_UNCHANGED`     | Price same in 2025 and 2026, matches both| ✅ None |
| `OLD_PRICE_2025`      | Billed at last year's rate               | ⚠️ Rebill or approve |
| `NO_MATCH`            | Price doesn't match either year          | 🔴 Investigate |
| `CUSTOM_PRICING`      | Contract-based pricing, no standard rate | 🔍 Manual verify |
| `NO_PRICEBOOK_CURRENCY` | Currency not in pricebook for this SKU | 📋 Pricebook gap |
| `BILLED_AT_ZERO`      | Net value is $0                          | 🔍 Intentional? |
| `ZERO_QTY_FLAT_PRICE` | One-time fee, no quantity tier           | 🔍 Flat price check |
| `CREDIT`              | Negative net value                       | 📋 Credit memo |
| `NOT_IN_PRICEBOOK`    | Material number not found at all         | 🔴 Investigate |

## Tech Stack

- **Python 3.10+**
- **pandas** — data cleaning, joins, aggregation
- **openpyxl** — Excel output formatting
- **pathlib** — cross-platform file handling

## Setup

**1. Install dependencies**
```bash
pip install pandas openpyxl
```

**2. Clone the repo**
```bash
git clone https://github.com/YOUR_USERNAME/billing-audit-pipeline.git
cd billing-audit-pipeline
```

**3. Configure file paths**

Open `run_audit.py` and set the three paths at the top:
```python
PRICEBOOK_FILE  = r"C:\path\to\your\pricebook.xlsx"
SAP_FILE        = r"C:\path\to\your\sap_export.xlsx"
OUTPUT_FILE     = r"C:\path\to\your\billing_audit.xlsx"
```

**4. Run**
```bash
python run_audit.py
```

That's it. The pipeline runs all three stages, validates inputs and outputs, and saves the audit workbook.

## Output

The audit workbook contains four tabs:

- **Needs_Review** — flagged rows only, primary working tab for the billing team
- **Correct** — all correctly billed rows for reference
- **Full_Data** — every row with all audit columns
- **Summary** — counts and dollar totals by flag

A `run_log.txt` is also appended after every run for month-over-month tracking.

## Monthly Workflow

1. Export SAP billing data to Excel
2. Update `SAP_FILE` path in `run_audit.py` if filename changed
3. Run `python run_audit.py`
4. Open `billing_audit.xlsx`, review `Needs_Review` tab

If the pricebook has been updated, re-run from Stage 1. The pricebook cleaner handles multi-tab, mixed-format workbooks automatically.

## Pricebook Handling

The pricebook cleaner auto-detects:
- **Tiered pricing** (Min/Max band columns)
- **Seat-based pricing** (seat count tiers)
- **Flat pricing** (no tier, fixed rate)
- **Custom/contract pricing** ("Custom" or "Pricing based on contract" cells)
- **Currency symbols** (£, €, A$, NZ$, R) in addition to ISO codes

## Validation

The `validation.py` module runs checks between every stage:
- File existence and write permissions before opening anything
- Required column assertions with plain-English fix suggestions
- Row count sanity checks to catch accidentally filtered exports
- Currency code validation
- Join integrity check — output row count must match SAP input exactly

If any check fails, the pipeline stops immediately with a clear message explaining what went wrong and how to fix it.

## Roadmap

- [ ] Regression test dataset (anonymized synthetic data)
- [ ] rapidfuzz fuzzy matching on material descriptions
- [ ] Streamlit web UI for non-technical users
- [ ] Month-over-month variance reporting

## Sample Data

Anonymized synthetic sample data is available in `/sample_data/` for testing.  
See `generate_sample_data.py` to regenerate with different random seeds.

**No real customer, pricing, or billing data is committed to this repository.**

## Author

Nehemiah Brown 
Built as part of an ongoing billing automation initiative at IDEXX Laboratories
