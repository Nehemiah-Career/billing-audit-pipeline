"""
generate_sample_data.py
========================
Generates anonymized synthetic sample data for testing the billing audit pipeline.
All customer names, material numbers, and prices are randomized — no real data.

Run: python generate_sample_data.py
Output: sample_data/sample_pricebook.xlsx and sample_data/sample_sap_export.xlsx
"""

import pandas as pd
import random
import string
from pathlib import Path

SEED = 42
random.seed(SEED)

OUTPUT_DIR = Path('sample_data')
OUTPUT_DIR.mkdir(exist_ok=True)

# ---- SYNTHETIC MATERIAL NUMBERS ----
# Format mirrors real structure but all numbers are randomized
def fake_material():
    return f"XX-{random.randint(1000000, 9999999):07d}-00"

MATERIALS = [fake_material() for _ in range(12)]

PRODUCTS = [
    "Synthetic Subscription Small",
    "Synthetic Subscription Medium",
    "Synthetic Subscription Large",
    "Synthetic Support Basic",
    "Synthetic Support Premium",
    "Synthetic Implementation Fee",
    "Synthetic Module Add-on A",
    "Synthetic Module Add-on B",
    "Synthetic Trial Subscription",
    "Synthetic Hardware Support",
    "Synthetic SMS Add-on",
    "Synthetic Data Migration Fee",
]

CURRENCIES = ['USD', 'CAD', 'GBP', 'AUD', 'NZD']

# ---- SYNTHETIC PRICEBOOK ----
def generate_pricebook():
    rows = []

    for mat, product in zip(MATERIALS, PRODUCTS):
        # Each product has 3-5 tier bands
        base_price_usd = random.choice([49, 99, 149, 199, 299, 499, 999])
        tiers = sorted(random.sample(range(5, 500), 4))
        tiers.append(9999)  # max band

        for i, max_band in enumerate(tiers):
            # Price scales with tier (volume discount)
            tier_multiplier = 1 + (i * 0.15)
            price_2025_usd = round(base_price_usd * tier_multiplier, 2)
            # 2026 price is 2025 + 3-8% increase
            increase = random.uniform(1.03, 1.08)
            price_2026_usd = round(price_2025_usd * increase, 2)

            # FX rates (synthetic — not real rates)
            fx = {'USD': 1.0, 'CAD': 1.32, 'GBP': 0.79, 'AUD': 1.53, 'NZD': 1.63}

            for currency, rate in fx.items():
                rows.append({
                    'IDEXX Part Number': mat,
                    'SAP Description':   product,
                    'Min of Tier':       tiers[i-1] + 1 if i > 0 else 1,
                    'Max of Tier':       max_band,
                    f'US List Price USD (1/1/2025 - 12/31/2025)':   round(price_2025_usd, 2),
                    f'US List Price USD (beginning 1/1/2026)':       round(price_2026_usd, 2),
                    f'Canada List Price CAD (1/1/2025 - 12/31/2025)': round(price_2025_usd * fx['CAD'], 2),
                    f'Canada List Price CAD (beginning 1/1/2026)':    round(price_2026_usd * fx['CAD'], 2),
                    f'UK List Price GBP (1/1/2025 - 12/31/2025)':    round(price_2025_usd * fx['GBP'], 2),
                    f'UK List Price GBP (beginning 1/1/2026)':        round(price_2026_usd * fx['GBP'], 2),
                    f'AUS List Price AUD (1/1/2025 - 12/31/2025)':   round(price_2025_usd * fx['AUD'], 2),
                    f'AUS List Price AUD (beginning 1/1/2026)':       round(price_2026_usd * fx['AUD'], 2),
                    f'NZ List Price NZD (1/1/2025 - 12/31/2025)':    round(price_2025_usd * fx['NZD'], 2),
                    f'NZ List Price NZD (beginning 1/1/2026)':        round(price_2026_usd * fx['NZD'], 2),
                })

    # Store prices per material for SAP generation
    return pd.DataFrame(rows)

# ---- SYNTHETIC SAP EXPORT ----
def fake_customer():
    adjectives = ['Northern', 'Central', 'Valley', 'Coastal', 'Highland', 'River']
    nouns = ['Animal Hospital', 'Veterinary Clinic', 'Pet Care', 'Vet Services', 'Animal Care']
    return f"{random.choice(adjectives)} {random.choice(nouns)}"

def generate_sap(pricebook_df):
    rows = []
    order_num = 1000000

    # Known error types for test cases
    error_scenarios = {
        'old_price':     2,   # billed at 2025 price
        'wrong_amount':  3,   # billed at wrong price
        'billed_at_zero': 2,  # charged $0
        'credit':        1,   # negative value
    }

    for mat, product in zip(MATERIALS, PRODUCTS):
        pb_mat = pricebook_df[pricebook_df['IDEXX Part Number'] == mat]
        if pb_mat.empty:
            continue

        currency = random.choice(CURRENCIES)
        qty = random.choice([5, 15, 50, 100, 250])

        # Find correct tier row
        col_map = {
            'USD': ('US List Price USD (1/1/2025 - 12/31/2025)',
                    'US List Price USD (beginning 1/1/2026)'),
            'CAD': ('Canada List Price CAD (1/1/2025 - 12/31/2025)',
                    'Canada List Price CAD (beginning 1/1/2026)'),
            'GBP': ('UK List Price GBP (1/1/2025 - 12/31/2025)',
                    'UK List Price GBP (beginning 1/1/2026)'),
            'AUD': ('AUS List Price AUD (1/1/2025 - 12/31/2025)',
                    'AUS List Price AUD (beginning 1/1/2026)'),
            'NZD': ('NZ List Price NZD (1/1/2025 - 12/31/2025)',
                    'NZ List Price NZD (beginning 1/1/2026)'),
        }

        col_2025, col_2026 = col_map[currency]
        tier_row = pb_mat[pb_mat['Max of Tier'] >= qty].sort_values('Max of Tier').iloc[0]
        correct_price = tier_row[col_2026]
        old_price     = tier_row[col_2025]

        # Decide scenario — mostly correct, some errors
        scenario_roll = random.random()
        if scenario_roll < 0.65:
            net_value = correct_price        # CORRECT_2026
            scenario  = 'correct'
        elif scenario_roll < 0.75:
            net_value = old_price            # OLD_PRICE_2025
            scenario  = 'old_price'
        elif scenario_roll < 0.85:
            net_value = correct_price * qty  # qty * price match
            scenario  = 'qty_match'
        elif scenario_roll < 0.90:
            net_value = round(correct_price * random.uniform(0.7, 1.3), 2)  # NO_MATCH
            scenario  = 'no_match'
        elif scenario_roll < 0.95:
            net_value = 0.0                  # BILLED_AT_ZERO
            scenario  = 'zero'
        else:
            net_value = round(-correct_price * random.uniform(0.1, 0.5), 2)  # CREDIT
            scenario  = 'credit'

        rows.append({
            'SOrg.':          random.choice(['USS7', 'CAS1', 'UKS1', 'AUS1', 'NZS2']),
            'CreatedOn':      f"2026-01-{random.randint(1,28):02d}",
            'Order#':         str(order_num),
            'Ship-to':        str(random.randint(100000, 999999)),
            'Name 1':         fake_customer(),
            'Address':        f"{random.randint(1,999)} Synthetic St",
            'St.':            random.choice(['A', 'B', 'C']),
            'Sold to':        str(random.randint(100000, 999999)),
            'Material':       mat,
            'Description':    product,
            'Order quantity': qty,
            'Net Value':      f"{net_value:,.2f}",
            'Curr.':          currency,
            'CGp':            random.choice(['C', 'D', 'E']),
            # Hidden column — expected flag for test validation
            '_expected_flag': scenario,
        })
        order_num += 1

    return pd.DataFrame(rows)


def main():
    print("Generating synthetic sample data...")

    pb = generate_pricebook()
    sap = generate_sap(pb)

    # Save pricebook — one tab per product group (simplified single tab for sample)
    pb_out = OUTPUT_DIR / 'sample_pricebook.xlsx'
    with pd.ExcelWriter(pb_out, engine='openpyxl') as writer:
        pb.to_excel(writer, sheet_name='Sample Products', index=False)
    print(f"  Pricebook: {pb_out}  ({len(pb):,} rows)")

    # Save SAP — drop the hidden _expected_flag column for the actual test file
    sap_public = sap.drop(columns=['_expected_flag'])
    sap_out = OUTPUT_DIR / 'sample_sap_export.xlsx'
    sap_public.to_excel(sap_out, index=False)
    print(f"  SAP export: {sap_out}  ({len(sap):,} rows)")

    # Save expected results separately for regression testing
    expected_out = OUTPUT_DIR / 'expected_flags.csv'
    sap[['Material', 'Order quantity', 'Curr.', 'Net Value', '_expected_flag']].to_csv(
        expected_out, index=False
    )
    print(f"  Expected flags: {expected_out}")
    print("\nDone. No real data was used — all values are synthetic.")
    print("Safe to commit sample_data/ to GitHub.")


if __name__ == '__main__':
    main()
