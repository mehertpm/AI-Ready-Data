"""
Banking Oracle Simulation — SQLite DB with intentional DQ issues
Tables: CUSTOMERS, ACCOUNTS, TRANSACTIONS, CARDS, LOANS, BENEFICIARIES, RISK_EVENTS, BRANCHES
"""
import sqlite3, random, string
from pathlib import Path
from datetime import datetime, timedelta

DB_PATH = Path(__file__).parent / "banking.db"

TABLE_ORDER = [
    "CUSTOMERS", "ACCOUNTS", "TRANSACTIONS", "CARDS",
    "LOANS", "BENEFICIARIES", "RISK_EVENTS", "BRANCHES"
]

READINESS_SCORES = {
    "CUSTOMERS":     {"source": 38, "bronze": 45, "silver": 68, "gold": 87, "platinum": 96},
    "ACCOUNTS":      {"source": 44, "bronze": 50, "silver": 72, "gold": 89, "platinum": 97},
    "TRANSACTIONS":  {"source": 35, "bronze": 42, "silver": 66, "gold": 85, "platinum": 95},
    "CARDS":         {"source": 41, "bronze": 48, "silver": 70, "gold": 88, "platinum": 96},
    "LOANS":         {"source": 40, "bronze": 47, "silver": 69, "gold": 86, "platinum": 95},
    "BENEFICIARIES": {"source": 36, "bronze": 43, "silver": 65, "gold": 84, "platinum": 94},
    "RISK_EVENTS":   {"source": 30, "bronze": 38, "silver": 62, "gold": 83, "platinum": 95},
    "BRANCHES":      {"source": 55, "bronze": 61, "silver": 78, "gold": 91, "platinum": 97},
}

COMPLIANCE_SCORES = {
    "CUSTOMERS":     {"source": 32, "bronze": 40, "silver": 67, "gold": 90, "platinum": 99},
    "ACCOUNTS":      {"source": 45, "bronze": 52, "silver": 74, "gold": 91, "platinum": 99},
    "TRANSACTIONS":  {"source": 28, "bronze": 36, "silver": 61, "gold": 87, "platinum": 98},
    "CARDS":         {"source": 35, "bronze": 42, "silver": 65, "gold": 88, "platinum": 99},
    "LOANS":         {"source": 38, "bronze": 45, "silver": 68, "gold": 89, "platinum": 98},
    "BENEFICIARIES": {"source": 30, "bronze": 38, "silver": 60, "gold": 85, "platinum": 97},
    "RISK_EVENTS":   {"source": 22, "bronze": 30, "silver": 55, "gold": 82, "platinum": 97},
    "BRANCHES":      {"source": 50, "bronze": 57, "silver": 75, "gold": 92, "platinum": 99},
}

SCORE_FACTORS = {
    "CUSTOMERS":     {"kyc_completeness": 30, "dedup_integrity": 20, "format_validity": 20, "referential_integrity": 15, "aml_readiness": 15},
    "ACCOUNTS":      {"balance_integrity": 25, "product_completeness": 25, "regulatory_flags": 20, "referential_integrity": 20, "freshness": 10},
    "TRANSACTIONS":  {"transaction_integrity": 30, "aml_patterns": 25, "merchant_completeness": 20, "temporal_coherence": 15, "dedup": 10},
    "CARDS":         {"card_validity": 30, "pci_compliance": 25, "status_accuracy": 20, "referential_integrity": 15, "freshness": 10},
    "LOANS":         {"collateral_completeness": 25, "payment_coherence": 25, "regulatory_compliance": 25, "referential_integrity": 15, "freshness": 10},
    "BENEFICIARIES": {"verification_status": 30, "routing_validity": 25, "kyc_linkage": 20, "dedup": 15, "format_validity": 10},
    "RISK_EVENTS":   {"resolution_completeness": 30, "sar_timeliness": 25, "pattern_coverage": 20, "regulatory_linkage": 15, "freshness": 10},
    "BRANCHES":      {"operational_completeness": 25, "regulatory_compliance": 25, "geo_accuracy": 20, "staff_linkage": 20, "freshness": 10},
}

TABLE_DQ_ISSUES = {
    "CUSTOMERS":     {"missing_ssn": "15% of customers missing SSN/TIN", "duplicate_customers": "8% duplicate customer records (name+DOB match)", "invalid_email": "12% malformed email addresses", "missing_kyc_date": "20% missing KYC verification date", "address_incomplete": "18% incomplete address (missing ZIP or state)"},
    "ACCOUNTS":      {"negative_balance": "5% accounts with unexplained negative balances", "missing_rate": "12% savings/CD accounts missing interest rate", "dormant_unflagged": "9% dormant accounts (>12mo no activity) not flagged", "currency_mismatch": "4% transactions in wrong currency for account type", "missing_product_code": "7% accounts missing product classification code"},
    "TRANSACTIONS":  {"duplicate_txn_id": "3% duplicate transaction IDs across ledger", "missing_merchant": "10% transactions missing merchant category code", "suspicious_round": "8% suspiciously round-number cash transactions (structuring risk)", "future_dated": "2% transactions dated in the future", "orphaned_txn": "5% transactions referencing closed/non-existent accounts"},
    "CARDS":         {"expired_active": "15% expired cards still showing Active status", "invalid_luhn": "5% card numbers failing Luhn algorithm check", "missing_cvv_hash": "20% cards missing CVV hash (PCI-DSS gap)", "overlapping_limits": "6% cards with credit limit exceeding account approved limit", "unlinked_cards": "4% cards not linked to any valid account"},
    "LOANS":         {"missing_collateral": "10% secured loans missing collateral appraisal value", "future_payment": "7% payment schedules with dates set in the past (missed)", "rate_outlier": "9% loans with interest rates outside product policy range (±3σ)", "missing_ltv": "14% mortgage loans missing LTV ratio", "covenant_missing": "11% commercial loans missing covenant tracking fields"},
    "BENEFICIARIES": {"missing_bic": "22% international beneficiaries missing BIC/SWIFT code", "unverified": "18% beneficiaries not completed verification workflow", "stale_routing": "12% US beneficiaries with routing numbers flagged as invalid by Fed", "kyc_gap": "15% beneficiaries not linked to a verified customer", "duplicate_bene": "6% duplicate beneficiary entries per customer"},
    "RISK_EVENTS":   {"unresolved_alerts": "30% AML/fraud alerts open beyond SLA (>30 days)", "missing_sar": "25% Suspicious Activity Reports missing regulatory filing date", "incomplete_narrative": "35% SAR narratives below minimum word count (FINCEN requirement)", "unscored": "20% risk events missing risk score/severity", "missing_resolution": "28% resolved events missing resolution outcome code"},
    "BRANCHES":      {"inactive_processing": "5% inactive/closed branches still processing transactions", "missing_manager": "8% branches missing assigned manager ID", "address_mismatch": "7% branch addresses don't match regulatory filing addresses", "missing_swift": "12% international branches missing SWIFT BIC code", "license_expired": "3% branches with expired operating licenses"},
}

PHI_DETECTED = {
    "CUSTOMERS":     {"ssn": True, "dob": True, "full_name": True, "address": True, "phone": True, "email": True, "account_number": True, "tax_id": True},
    "ACCOUNTS":      {"account_number": True, "balance": True, "routing_number": True, "product_type": True},
    "TRANSACTIONS":  {"account_number": True, "amount": True, "merchant_name": True, "card_last4": True},
    "CARDS":         {"card_number": True, "cvv_hash": True, "expiry": True, "cardholder_name": True},
    "LOANS":         {"account_number": True, "ssn": True, "income": True, "collateral_value": True},
    "BENEFICIARIES": {"full_name": True, "account_number": True, "routing_number": True, "address": True},
    "RISK_EVENTS":   {"account_number": True, "amount": True, "alert_type": True},
    "BRANCHES":      {"address": True, "phone": True, "manager_name": True},
}

IMPROVEMENT_PATHS = {
    "CUSTOMERS": {
        "bronze_to_silver": [
            "Standardize name fields to UPPER CASE Title format",
            "Validate and normalize email format (RFC 5322)",
            "Geocode addresses and fill missing ZIP/state",
            "De-duplicate using name + DOB + SSN last4 match key",
            "Parse phone numbers to E.164 format",
        ],
        "silver_to_gold": [
            "Complete KYC verification for 100% of active customers",
            "Populate missing SSN/TIN from secure vault linkage",
            "Run AML customer risk scoring (CDD/EDD segmentation)",
            "Link all customers to at least one verified account",
            "Apply OFAC/PEP screening flags",
        ],
        "gold_to_platinum": [
            "Implement real-time KYC refresh triggers on change events",
            "Enable CIP (Customer Identification Program) audit trail",
            "Generate ML-ready customer 360 feature vectors",
            "Apply differential privacy on aggregate analytics exports",
            "Achieve SOX + GLBA + CCPA tagging on all PII fields",
        ],
    },
    "ACCOUNTS": {
        "bronze_to_silver": [
            "Reconcile negative balances against transaction ledger",
            "Populate missing interest rates from product master table",
            "Classify and flag dormant accounts per Reg E / state escheatment rules",
            "Standardize product codes to ISO 20022 product taxonomy",
            "Resolve currency mismatches via cross-reference to account agreement",
        ],
        "silver_to_gold": [
            "Implement daily balance reconciliation vs. GL entries",
            "Apply FDIC insurance coverage calculation per account",
            "Flag accounts exceeding Reg D withdrawal limits",
            "Enforce referential integrity: every account → valid customer",
            "Generate account-level risk tier (Low / Medium / High)",
        ],
        "gold_to_platinum": [
            "Enable real-time balance streaming to data lakehouse",
            "Apply ML propensity scores for churn, upsell, attrition",
            "Full Basel III capital adequacy classification per account class",
            "Automate Regulation CC hold calculation engine",
            "Achieve T+0 reconciliation with core banking system",
        ],
    },
    "TRANSACTIONS": {
        "bronze_to_silver": [
            "Deduplicate on transaction_id + timestamp + amount composite key",
            "Populate missing merchant category codes via MCC lookup table",
            "Correct future-dated transactions via audit log reconciliation",
            "Remove orphaned transactions (no linked account)",
            "Normalize transaction types to ISO 8583 message types",
        ],
        "silver_to_gold": [
            "Apply ML-based structuring detection (CTR $10K threshold monitoring)",
            "Tag transactions with SWIFT purpose codes for cross-border",
            "Implement AML velocity rules (24h, 7d, 30d rolling windows)",
            "Link all transactions to beneficiary records for wire transfers",
            "Enable SAR auto-trigger on pattern match (>3 suspicious signals)",
        ],
        "gold_to_platinum": [
            "Real-time fraud scoring via feature store (sub-100ms latency)",
            "Graph analytics for money laundering network detection",
            "Embed ISO 20022 pain.001 / camt.054 message compliance",
            "Integrate SWIFT gpi tracking for cross-border transactions",
            "Achieve FinCEN BSA/AML full audit trail certification",
        ],
    },
    "CARDS": {
        "bronze_to_silver": [
            "Revalidate all card numbers using Luhn algorithm; invalidate failures",
            "Update expired cards to Expired status in card master",
            "Populate missing CVV hashes (PCI-DSS Req 3.3 tokenization)",
            "Resolve credit limit overages against account credit policy",
            "Link all unlinked cards to valid account via card agreement",
        ],
        "silver_to_gold": [
            "Apply PCI-DSS Level 1 tokenization on all PAN fields",
            "Implement card lifecycle state machine (Active→Suspended→Expired→Closed)",
            "Enable EMV 3DS2 authentication flag per card",
            "Cross-reference card spend with account transaction history",
            "Flag cards with velocity anomalies for fraud review queue",
        ],
        "gold_to_platinum": [
            "Real-time authorization scoring with ML fraud model",
            "Full PCI-DSS ROC (Report on Compliance) audit readiness",
            "Network tokenization (Visa Token Service / Mastercard MDES) linkage",
            "Geolocation anomaly detection on card-present transactions",
            "Zero-trust card data vault with HSM-backed key management",
        ],
    },
    "LOANS": {
        "bronze_to_silver": [
            "Source missing collateral appraisal values from origination system",
            "Recalculate LTV ratios using current assessed property values",
            "Flag past-due payment schedules and generate delinquency codes",
            "Normalize interest rates and flag policy exceptions for review",
            "Complete covenant tracking fields for all commercial loans",
        ],
        "silver_to_gold": [
            "Apply CECL (Current Expected Credit Loss) reserve calculation",
            "Enable HMDA (Home Mortgage Disclosure Act) reporting fields",
            "Run credit concentration analysis by borrower / sector / geography",
            "Generate DSCR (Debt Service Coverage Ratio) for commercial loans",
            "Link all loans to CRA (Community Reinvestment Act) assessment areas",
        ],
        "gold_to_platinum": [
            "ML-based probability of default (PD) scoring per loan",
            "Stress testing integration (DFAST / CCAR scenario analysis)",
            "Real-time covenant breach monitoring with alert triggers",
            "Automate HMDA LAR (Loan Application Register) submission",
            "Achieve full Basel III IRB approach data readiness",
        ],
    },
    "BENEFICIARIES": {
        "bronze_to_silver": [
            "Source and populate BIC/SWIFT codes from SWIFT BIC directory",
            "Initiate micro-deposit verification workflow for unverified beneficiaries",
            "Validate US routing numbers against Fed ACH participant list",
            "De-duplicate beneficiaries per customer on name + routing + account",
            "Link unlinked beneficiaries to verified customer via relationship table",
        ],
        "silver_to_gold": [
            "Apply OFAC SDN list screening on all beneficiaries",
            "Implement beneficiary risk scoring (domestic / international / high-risk jurisdiction)",
            "Require enhanced due diligence for beneficiaries in FATF grey/black list countries",
            "Enable beneficiary change audit trail (who changed what and when)",
            "Cross-reference against FinCEN 314(b) information sharing",
        ],
        "gold_to_platinum": [
            "Real-time sanctions screening via Thomson Reuters / Dow Jones API",
            "Graph-based beneficial ownership mapping (UBO detection)",
            "ISO 20022 PACS.008 compliant beneficiary data model",
            "Continuous monitoring for beneficiary risk re-scoring",
            "Full FATF Recommendation 16 (Wire Transfer Rule) compliance",
        ],
    },
    "RISK_EVENTS": {
        "bronze_to_silver": [
            "Auto-assign SLA breach flags to all alerts open >30 days",
            "Populate SAR filing dates from case management system backfill",
            "Enrich narratives using structured template to meet FINCEN word count",
            "Apply risk scores to all unscored events using rule-based engine",
            "Populate resolution outcome codes from case closure records",
        ],
        "silver_to_gold": [
            "Build ML alert prioritization model to reduce false positive rate (target <30%)",
            "Implement STR/SAR automated narrative generation using LLM",
            "Enable real-time alert deduplication using graph entity resolution",
            "Link all SAR filings to BSA E-Filing system confirmation numbers",
            "Create typology tagging (layering, structuring, smurfing, etc.)",
        ],
        "gold_to_platinum": [
            "Network graph analytics for transaction laundering detection",
            "Explainable AI (XAI) on all ML-generated risk scores for regulator review",
            "Automated SAR filing to FinCEN via BSA E-Filing API",
            "Federated AML model sharing across consortium banks",
            "Achieve ACAMS-certified AML program data layer certification",
        ],
    },
    "BRANCHES": {
        "bronze_to_silver": [
            "Flag and suspend transaction processing on closed/inactive branches",
            "Assign manager IDs by querying HR system branch assignment table",
            "Reconcile addresses against USPS AIS / international postal APIs",
            "Populate SWIFT BIC codes for all international branch locations",
            "Flag expired operating licenses for renewal workflow trigger",
        ],
        "silver_to_gold": [
            "Link branches to regulatory reporting jurisdictions (OCC / FDIC / state)",
            "Enable CRA assessment area mapping per branch footprint",
            "Implement branch-level capacity and transaction volume metrics",
            "Cross-reference to call report (FFIEC 041/051) branch schedule",
            "Apply BSA/AML program coverage mapping per branch risk tier",
        ],
        "gold_to_platinum": [
            "Real-time branch performance dashboards with regulatory SLA tracking",
            "Automated FDIC branch summary (FFIEC 002) submission readiness",
            "Geospatial analytics for CRA LMI (Low-Moderate Income) coverage",
            "Branch risk scoring integration with enterprise risk framework",
            "Achieve full OCC examination readiness data package automation",
        ],
    },
}

PCI_IDENTIFIERS = [
    "Primary Account Number (PAN)",
    "Cardholder Name",
    "Card Expiration Date",
    "CVV / CVC / CID",
    "PIN / PIN Block",
    "Magnetic Stripe Data",
    "Chip Data (ICC)",
    "Card Billing Address",
    "Card Verification Value",
    "Token (surrogate PAN)",
]

AML_KYC_FIELDS = [
    "Full Legal Name",
    "Date of Birth",
    "Government ID (SSN/TIN/Passport)",
    "Physical Address",
    "Email / Phone",
    "Source of Funds",
    "Beneficial Ownership",
    "PEP Status",
    "OFAC Screening Result",
    "CDD Risk Rating",
    "EDD Documentation",
    "Transaction Monitoring Profile",
]


def _rnd_str(n):
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=n))


def _rnd_date(a=3650, b=0):
    lo, hi = min(a, b), max(a, b)
    if lo == hi: hi += 1
    d = datetime.now() - timedelta(days=random.randint(lo, hi))
    return d.strftime("%Y-%m-%d")


def _rnd_datetime(a=730, b=0):
    lo, hi = min(a, b), max(a, b)
    if lo == hi: hi += 1
    d = datetime.now() - timedelta(
        days=random.randint(lo, hi),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
    )
    return d.strftime("%Y-%m-%d %H:%M:%S")


def create_database():
    if DB_PATH.exists():
        return
    random.seed(42)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # CUSTOMERS
    cur.execute("""CREATE TABLE IF NOT EXISTS CUSTOMERS (
        customer_id TEXT PRIMARY KEY,
        first_name TEXT, last_name TEXT,
        dob TEXT, ssn TEXT, tax_id TEXT,
        email TEXT, phone TEXT,
        address_line1 TEXT, address_line2 TEXT,
        city TEXT, state TEXT, zip_code TEXT,
        country TEXT DEFAULT 'US',
        kyc_date TEXT, kyc_status TEXT,
        risk_tier TEXT, pep_flag INTEGER DEFAULT 0,
        ofac_screened INTEGER DEFAULT 0,
        created_at TEXT, updated_at TEXT
    )""")

    first_names = ["James","Mary","John","Patricia","Robert","Jennifer","Michael","Linda","William","Barbara"]
    last_names  = ["Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis","Wilson","Anderson"]
    states = ["CA","NY","TX","FL","IL","PA","OH","GA","NC","MI"]

    customers = []
    for i in range(5000):
        cid = f"CUST{i+1:06d}"
        fn = random.choice(first_names)
        ln = random.choice(last_names)
        ssn = None if random.random() < 0.15 else f"{random.randint(100,999)}-{random.randint(10,99)}-{random.randint(1000,9999)}"
        email = None if random.random() < 0.12 else (
            f"{fn.lower()}.{ln.lower()}{random.randint(1,999)}@example.com"
            if random.random() > 0.05 else f"INVALID_EMAIL_{i}"
        )
        kyc_date = None if random.random() < 0.20 else _rnd_date(1825, 30)
        kyc_status = "VERIFIED" if kyc_date else ("PENDING" if random.random() > 0.3 else None)
        zip_code = None if random.random() < 0.18 else f"{random.randint(10000,99999)}"
        state = None if random.random() < 0.10 else random.choice(states)
        customers.append((
            cid, fn, ln,
            _rnd_date(25550, 6570),  # DOB 18-70 years ago
            ssn, f"TIN{random.randint(100000000,999999999)}",
            email, f"+1{random.randint(2000000000,9999999999)}",
            f"{random.randint(100,9999)} {random.choice(['Main','Oak','Elm','Park'])} St",
            None, random.choice(["New York","Los Angeles","Chicago","Houston","Phoenix"]),
            state, zip_code, "US",
            kyc_date, kyc_status,
            random.choice(["LOW","MEDIUM","HIGH",None]),
            1 if random.random() < 0.02 else 0,
            1 if random.random() < 0.85 else 0,
            _rnd_datetime(3650, 365), _rnd_datetime(365, 0),
        ))
    # inject 8% duplicates
    dupes = random.sample(customers, int(len(customers)*0.08))
    for d in dupes:
        c = list(d)
        c[0] = f"CUST{random.randint(90000,99999)}"
        customers.append(tuple(c))

    cur.executemany("INSERT OR IGNORE INTO CUSTOMERS VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", customers)

    # ACCOUNTS
    cur.execute("""CREATE TABLE IF NOT EXISTS ACCOUNTS (
        account_id TEXT PRIMARY KEY,
        customer_id TEXT,
        account_type TEXT, product_code TEXT,
        balance REAL, available_balance REAL,
        currency TEXT DEFAULT 'USD',
        interest_rate REAL,
        opened_date TEXT, last_activity_date TEXT,
        status TEXT, dormant_flag INTEGER DEFAULT 0,
        routing_number TEXT, iban TEXT,
        branch_id TEXT, credit_limit REAL,
        created_at TEXT
    )""")

    acct_types = ["CHECKING","SAVINGS","CREDIT","MONEY_MARKET","CD","MORTGAGE","AUTO_LOAN"]
    product_codes = ["CHK001","SAV001","CRD001","MMA001","CD001","MTG001","AUTO001"]
    accounts = []
    for i in range(8000):
        aid = f"ACCT{i+1:07d}"
        cid = f"CUST{random.randint(1,5000):06d}"
        atype = random.choice(acct_types)
        pcode = product_codes[acct_types.index(atype)] if random.random() > 0.07 else None
        bal = round(random.uniform(-500, 250000), 2) if random.random() < 0.05 else round(random.uniform(0, 250000), 2)
        avail = max(0, bal - random.uniform(0, 500))
        rate = None if (atype in ["SAVINGS","CD","MONEY_MARKET","MORTGAGE","AUTO_LOAN"] and random.random() < 0.12) else round(random.uniform(0.01, 18.99), 2)
        last_act = _rnd_date(1825, 0)
        dormant = 1 if (datetime.now() - datetime.strptime(last_act, "%Y-%m-%d")).days > 365 and random.random() < 0.7 else 0
        dormant_flag = 0 if dormant and random.random() < 0.9 else dormant  # 90% not properly flagged
        accounts.append((
            aid, cid, atype, pcode,
            bal, round(avail, 2), "USD",
            rate, _rnd_date(3650, 365), last_act,
            random.choice(["ACTIVE","ACTIVE","ACTIVE","SUSPENDED","CLOSED"]),
            dormant_flag,
            f"0{random.randint(21000000,99999999)}",
            f"US{random.randint(10,99)}{random.randint(1000000000,9999999999)}",
            f"BR{random.randint(1,200):04d}",
            round(random.uniform(500, 50000), 2) if atype == "CREDIT" else None,
            _rnd_datetime(3650, 0),
        ))
    cur.executemany("INSERT OR IGNORE INTO ACCOUNTS VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", accounts)

    # TRANSACTIONS
    cur.execute("""CREATE TABLE IF NOT EXISTS TRANSACTIONS (
        transaction_id TEXT PRIMARY KEY,
        account_id TEXT, related_account_id TEXT,
        transaction_type TEXT,
        amount REAL, currency TEXT DEFAULT 'USD',
        merchant_name TEXT, merchant_category_code TEXT,
        channel TEXT, status TEXT,
        transaction_date TEXT, value_date TEXT,
        reference TEXT, description TEXT,
        aml_flag INTEGER DEFAULT 0,
        created_at TEXT
    )""")

    txn_types = ["DEBIT","CREDIT","TRANSFER","WIRE","ACH","ATM","POS","BILL_PAY"]
    channels  = ["ONLINE","MOBILE","BRANCH","ATM","POS_TERMINAL","WIRE"]
    merchants = ["AMAZON","WALMART","STARBUCKS","HOME DEPOT","SHELL","MCDONALDS","TARGET","COSTCO",None]
    mcc_codes = ["5411","5912","5812","5310","5541","7011","4814","6011",None]
    transactions = []
    for i in range(100000):
        tid = f"TXN{i+1:09d}" if random.random() > 0.03 else f"TXN{random.randint(1,50000):09d}"  # 3% dupe IDs
        aid = f"ACCT{random.randint(1,8000):07d}" if random.random() > 0.05 else f"ACCT{random.randint(9000,9999):07d}"  # 5% orphan
        mname = random.choice(merchants)
        mcc = None if (mname is None or random.random() < 0.10) else mcc_codes[merchants.index(mname)] if mname in merchants else None
        amt = random.choice([10000.0, 9000.0, 5000.0, 4999.0]) if random.random() < 0.08 else round(random.uniform(0.50, 15000), 2)
        txn_date = _rnd_datetime(730, 0) if random.random() > 0.02 else _rnd_datetime(-1, -30)  # 2% future dated
        transactions.append((
            tid, aid, f"ACCT{random.randint(1,8000):07d}" if random.random() > 0.7 else None,
            random.choice(txn_types), amt, "USD",
            mname, mcc, random.choice(channels),
            random.choice(["COMPLETED","COMPLETED","COMPLETED","PENDING","FAILED"]),
            txn_date, txn_date,
            _rnd_str(12), f"Transaction {i+1}",
            1 if amt >= 9000 and random.random() < 0.3 else 0,
            _rnd_datetime(730, 0),
        ))
    cur.executemany("INSERT OR IGNORE INTO TRANSACTIONS VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", transactions)

    # CARDS
    cur.execute("""CREATE TABLE IF NOT EXISTS CARDS (
        card_id TEXT PRIMARY KEY,
        account_id TEXT, customer_id TEXT,
        card_number TEXT, card_type TEXT,
        cardholder_name TEXT,
        expiry_date TEXT, cvv_hash TEXT,
        status TEXT, credit_limit REAL,
        available_credit REAL,
        issued_date TEXT, last_used_date TEXT,
        created_at TEXT
    )""")

    card_types = ["VISA_DEBIT","MASTERCARD_DEBIT","VISA_CREDIT","MASTERCARD_CREDIT","AMEX_CREDIT"]
    cards = []
    for i in range(6000):
        crd_id = f"CARD{i+1:07d}"
        aid = f"ACCT{random.randint(1,8000):07d}" if random.random() > 0.04 else None
        cid = f"CUST{random.randint(1,5000):06d}"
        # Luhn invalid for ~5%
        pan = f"4{''.join([str(random.randint(0,9)) for _ in range(15)])}"
        if random.random() < 0.05:
            pan = pan[:-1] + str((int(pan[-1]) + 1) % 10)  # break luhn
        exp_year = random.randint(2021, 2029)
        exp_month = random.randint(1, 12)
        expiry = f"{exp_month:02d}/{exp_year}"
        is_expired = exp_year < 2025 or (exp_year == 2025 and exp_month < datetime.now().month)
        # 15% expired but still Active
        status = "EXPIRED" if (is_expired and random.random() > 0.15) else "ACTIVE"
        cvv_hash = None if random.random() < 0.20 else _rnd_str(64)
        cl = round(random.uniform(500, 25000), 2) if "CREDIT" in card_types[i % len(card_types)] else None
        cards.append((
            crd_id, aid, cid, pan,
            card_types[i % len(card_types)],
            f"{random.choice(first_names)} {random.choice(last_names)}",
            expiry, cvv_hash, status, cl,
            round(cl * random.uniform(0.1, 1.0), 2) if cl else None,
            _rnd_date(1825, 30), _rnd_date(730, 0),
            _rnd_datetime(1825, 0),
        ))
    cur.executemany("INSERT OR IGNORE INTO CARDS VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", cards)

    # LOANS
    cur.execute("""CREATE TABLE IF NOT EXISTS LOANS (
        loan_id TEXT PRIMARY KEY,
        customer_id TEXT, account_id TEXT,
        loan_type TEXT, principal_amount REAL,
        outstanding_balance REAL, interest_rate REAL,
        origination_date TEXT, maturity_date TEXT,
        next_payment_date TEXT, last_payment_date TEXT,
        payment_amount REAL, payment_frequency TEXT,
        collateral_type TEXT, collateral_value REAL,
        ltv_ratio REAL, credit_score INTEGER,
        delinquency_days INTEGER DEFAULT 0,
        status TEXT, created_at TEXT
    )""")

    loan_types = ["MORTGAGE","AUTO_LOAN","PERSONAL","HOME_EQUITY","COMMERCIAL","STUDENT"]
    loan_statuses = ["CURRENT","CURRENT","CURRENT","30_DAYS_PAST","60_DAYS_PAST","CHARGED_OFF","PAID_OFF"]
    loans = []
    for i in range(3000):
        lid = f"LOAN{i+1:07d}"
        ltype = random.choice(loan_types)
        principal = round(random.uniform(5000, 800000), 2)
        rate = round(random.uniform(0.5, 24.99), 2)
        # Flag outliers
        if random.random() < 0.09:
            rate = round(random.uniform(25, 45), 2)
        col_val = None if random.random() < 0.10 else round(principal * random.uniform(0.5, 1.5), 2)
        ltv = None if (ltype == "MORTGAGE" and random.random() < 0.14) else (round(principal / col_val * 100, 2) if col_val else None)
        next_pmt = _rnd_date(60, 1) if random.random() < 0.07 else _rnd_date(-1, -60)  # 7% past due (negative = future)
        loans.append((
            lid, f"CUST{random.randint(1,5000):06d}", f"ACCT{random.randint(1,8000):07d}",
            ltype, principal,
            round(principal * random.uniform(0.1, 0.99), 2),
            rate, _rnd_date(3650, 365), _rnd_date(-365, -30),
            next_pmt, _rnd_date(365, 30),
            round(principal * rate / 100 / 12, 2),
            random.choice(["MONTHLY","BIWEEKLY","QUARTERLY"]),
            random.choice(["REAL_ESTATE","VEHICLE","NONE","EQUIPMENT",None]) if ltype not in ["PERSONAL","STUDENT"] else "NONE",
            col_val, ltv,
            random.randint(580, 850),
            random.randint(0, 90),
            random.choice(loan_statuses),
            _rnd_datetime(3650, 0),
        ))
    cur.executemany("INSERT OR IGNORE INTO LOANS VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", loans)

    # BENEFICIARIES
    cur.execute("""CREATE TABLE IF NOT EXISTS BENEFICIARIES (
        beneficiary_id TEXT PRIMARY KEY,
        customer_id TEXT,
        full_name TEXT, relationship TEXT,
        bank_name TEXT, account_number TEXT,
        routing_number TEXT, bic_swift TEXT,
        bank_country TEXT, verified INTEGER DEFAULT 0,
        verification_date TEXT,
        ofac_screened INTEGER DEFAULT 0,
        risk_tier TEXT, created_at TEXT
    )""")

    bene_countries = ["US","US","US","US","MX","CA","GB","DE","IN","CN","NG","AE"]
    benes = []
    for i in range(4000):
        bid = f"BENE{i+1:07d}"
        cid = f"CUST{random.randint(1,5000):06d}" if random.random() > 0.15 else None
        country = random.choice(bene_countries)
        bic = None if (country != "US" and random.random() < 0.22) else f"BNKUS{_rnd_str(3)}" if country == "US" else f"BNK{country}{_rnd_str(3)}"
        routing = f"{random.randint(21000000,99999999)}" if country == "US" else None
        # 12% invalid routing
        if routing and random.random() < 0.12:
            routing = f"{random.randint(1,9999999):07d}"
        verified = 0 if random.random() < 0.18 else 1
        ver_date = None if not verified else _rnd_date(730, 30)
        benes.append((
            bid, cid,
            f"{random.choice(first_names)} {random.choice(last_names)}",
            random.choice(["FAMILY","BUSINESS","VENDOR","SELF",None]),
            f"Bank {_rnd_str(4)}", f"ACCT{random.randint(100000,999999)}",
            routing, bic, country, verified, ver_date,
            1 if random.random() < 0.75 else 0,
            random.choice(["LOW","MEDIUM","HIGH",None]),
            _rnd_datetime(1825, 0),
        ))
    cur.executemany("INSERT OR IGNORE INTO BENEFICIARIES VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", benes)

    # RISK_EVENTS
    cur.execute("""CREATE TABLE IF NOT EXISTS RISK_EVENTS (
        event_id TEXT PRIMARY KEY,
        customer_id TEXT, account_id TEXT, transaction_id TEXT,
        event_type TEXT, risk_score REAL,
        severity TEXT, alert_source TEXT,
        opened_date TEXT, resolved_date TEXT,
        resolution_code TEXT,
        sar_filed INTEGER DEFAULT 0,
        sar_filing_date TEXT,
        narrative TEXT,
        status TEXT, created_at TEXT
    )""")

    event_types = ["AML_VELOCITY","STRUCTURING","WIRE_FRAUD","ACCOUNT_TAKEOVER","SANCTIONS_HIT","CARD_FRAUD","IDENTITY_THEFT","UNUSUAL_PATTERN"]
    risk_events = []
    for i in range(2000):
        eid = f"RISK{i+1:07d}"
        opened = _rnd_datetime(730, 0)
        opened_dt = datetime.strptime(opened, "%Y-%m-%d %H:%M:%S")
        days_open = (datetime.now() - opened_dt).days
        is_open = random.random() < 0.45  # 45% still open
        resolved = None if is_open else (opened_dt + timedelta(days=random.randint(1, max(2, min(days_open, 90))))).strftime("%Y-%m-%d %H:%M:%S")
        res_code = None if is_open or random.random() < 0.28 else random.choice(["TRUE_POSITIVE","FALSE_POSITIVE","INCONCLUSIVE"])
        sar = 1 if (not is_open and random.random() < 0.3) else 0
        sar_date = None if not sar or random.random() < 0.25 else (opened_dt + timedelta(days=random.randint(10, 30))).strftime("%Y-%m-%d")
        risk_score = None if random.random() < 0.20 else round(random.uniform(10, 100), 1)
        severity = random.choice(["HIGH","HIGH","MEDIUM","MEDIUM","LOW"]) if risk_score else None
        # narratives may be too short
        narrative = ("Suspicious activity detected." if random.random() < 0.35 else
                     f"Customer flagged for {random.choice(event_types).lower().replace('_',' ')}. "
                     f"Multiple transactions observed over {random.randint(3,30)} day period totaling "
                     f"${random.randint(5000,500000):,}. Account activity inconsistent with customer profile.")
        risk_events.append((
            eid,
            f"CUST{random.randint(1,5000):06d}", f"ACCT{random.randint(1,8000):07d}",
            f"TXN{random.randint(1,100000):09d}",
            random.choice(event_types), risk_score, severity,
            random.choice(["SYSTEM_RULE","ML_MODEL","MANUAL","EXTERNAL_TIP"]),
            opened, resolved, res_code, sar, sar_date, narrative,
            "OPEN" if is_open else "CLOSED",
            _rnd_datetime(730, 0),
        ))
    cur.executemany("INSERT OR IGNORE INTO RISK_EVENTS VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", risk_events)

    # BRANCHES
    cur.execute("""CREATE TABLE IF NOT EXISTS BRANCHES (
        branch_id TEXT PRIMARY KEY,
        branch_name TEXT, branch_type TEXT,
        address TEXT, city TEXT, state TEXT,
        zip_code TEXT, country TEXT DEFAULT 'US',
        phone TEXT, manager_id TEXT,
        swift_bic TEXT, routing_number TEXT,
        status TEXT, license_number TEXT,
        license_expiry TEXT,
        opened_date TEXT, created_at TEXT
    )""")

    branch_types = ["RETAIL","COMMERCIAL","PRIVATE_BANKING","ONLINE_ONLY","INTERNATIONAL"]
    branches = []
    for i in range(200):
        bid = f"BR{i+1:04d}"
        btype = random.choice(branch_types)
        is_intl = btype == "INTERNATIONAL"
        status = "CLOSED" if random.random() < 0.05 else "ACTIVE"
        license_exp = _rnd_date(90, 1) if (random.random() < 0.03) else _rnd_date(-1, -730)
        branches.append((
            bid,
            f"{random.choice(['Main','Downtown','Uptown','North','South','East','West'])} Branch {i+1}",
            btype,
            f"{random.randint(100,9999)} {random.choice(['Main','Commerce','Financial','Bank'])} Blvd",
            random.choice(["New York","Los Angeles","Chicago","Houston","Miami"]),
            random.choice(states) if not is_intl else None,
            f"{random.randint(10000,99999)}" if not is_intl else None,
            "US" if not is_intl else random.choice(["GB","DE","SG","HK","CA"]),
            f"+1{random.randint(2000000000,9999999999)}",
            f"EMP{random.randint(1,1000):06d}" if random.random() > 0.08 else None,
            f"BNKUS{_rnd_str(3)}" if not is_intl else (None if random.random() < 0.12 else f"BNK{_rnd_str(6)}"),
            f"0{random.randint(21000000,99999999)}",
            status,
            f"LIC{_rnd_str(8)}",
            license_exp,
            _rnd_date(7300, 365),
            _rnd_datetime(7300, 0),
        ))
    cur.executemany("INSERT OR IGNORE INTO BRANCHES VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", branches)

    conn.commit()
    conn.close()
    print(f"Banking DB created: {DB_PATH}")


def get_table_stats():
    if not DB_PATH.exists():
        create_database()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    stats = {}
    domains = {
        "CUSTOMERS": "KYC / Customer Master",
        "ACCOUNTS": "Account Management",
        "TRANSACTIONS": "Transaction Ledger",
        "CARDS": "Card Services / PCI",
        "LOANS": "Loan Portfolio",
        "BENEFICIARIES": "Payment Beneficiaries",
        "RISK_EVENTS": "AML / Fraud Risk",
        "BRANCHES": "Branch Network",
    }
    regulatory = {
        "CUSTOMERS": "BSA / AML / KYC / GLBA",
        "ACCOUNTS": "FDIC / Reg E / Reg D / SOX",
        "TRANSACTIONS": "BSA / AML / CTR / FinCEN",
        "CARDS": "PCI-DSS Level 1 / Reg E",
        "LOANS": "HMDA / CECL / CRA / Basel III",
        "BENEFICIARIES": "FATF / OFAC / Reg E / ISO 20022",
        "RISK_EVENTS": "BSA / SAR / FinCEN / ACAMS",
        "BRANCHES": "OCC / FDIC / CRA / FFIEC",
    }
    for tbl in TABLE_ORDER:
        cur.execute(f"SELECT COUNT(*) FROM {tbl}")
        rows = cur.fetchone()[0]
        stats[tbl] = {
            "rows": rows,
            "domain": domains.get(tbl, ""),
            "regulatory": regulatory.get(tbl, ""),
            "display": tbl.replace("_", " ").title(),
        }
    conn.close()
    return stats
