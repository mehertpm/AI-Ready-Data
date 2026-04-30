"""
Simulates an Oracle ERP database using SQLite.
Generates realistic enterprise data with intentional quality issues
(nulls, duplicates, format errors, FK violations) for the pipeline to fix.
"""
import sqlite3
import random
import string
import datetime
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "oracle_sim.db")

FIRST_NAMES = ["James","Mary","John","Patricia","Robert","Jennifer","Michael","Linda",
               "William","Barbara","David","Susan","Richard","Jessica","Joseph","Sarah",
               "Thomas","Karen","Charles","Lisa","Christopher","Nancy","Daniel","Betty",
               "Matthew","Margaret","Anthony","Sandra","Mark","Ashley"]
LAST_NAMES = ["Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis",
              "Rodriguez","Martinez","Hernandez","Lopez","Gonzalez","Wilson","Anderson",
              "Thomas","Taylor","Moore","Jackson","Martin","Lee","Perez","Thompson","White"]
DOMAINS = ["gmail.com","yahoo.com","outlook.com","company.com","enterprise.org","corp.net"]
STATES = ["CA","NY","TX","FL","IL","PA","OH","GA","NC","MI","WA","AZ","MA","TN","IN"]
CITIES = ["New York","Los Angeles","Chicago","Houston","Phoenix","Philadelphia","San Antonio","San Diego"]
CATEGORIES = ["Electronics","Furniture","Office Supplies","Software","Hardware","Services","Consulting","Training"]
DEPARTMENTS = ["Engineering","Sales","Marketing","Finance","HR","Operations","Legal","IT","R&D"]
TXN_TYPES = ["DEBIT","CREDIT","TRANSFER","REFUND","ADJUSTMENT","REVERSAL"]
EVENT_TYPES = ["FRAUD_ALERT","LATE_PAYMENT","ACCOUNT_FREEZE","KYC_FAIL","CHARGEBACK","DISPUTE"]
SEVERITIES = ["LOW","MEDIUM","HIGH","CRITICAL"]
CONTRACT_STATUSES = ["ACTIVE","EXPIRED","PENDING","TERMINATED"]
CURRENCIES = ["USD","EUR","GBP","CAD","AUD","JPY","CHF"]
WAREHOUSES = ["WH-EAST-01","WH-WEST-02","WH-CENTRAL-03","WH-NORTH-04","WH-SOUTH-05"]

random.seed(42)


def _rand_date(start_year=2018, end_year=2024):
    start = datetime.date(start_year, 1, 1)
    end = datetime.date(end_year, 12, 31)
    delta = (end - start).days
    return (start + datetime.timedelta(days=random.randint(0, delta))).isoformat()


def _rand_phone():
    return f"+1-{random.randint(200,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}"


def _rand_ssn():
    return f"{random.randint(100,999)}-{random.randint(10,99)}-{random.randint(1000,9999)}"


def _rand_email(first, last):
    return f"{first.lower()}.{last.lower()}{random.randint(1,999)}@{random.choice(DOMAINS)}"


def _rand_cc():
    prefix = random.choice(["4","5","3"])
    return prefix + "".join([str(random.randint(0,9)) for _ in range(15)])


def _maybe_null(value, pct=0.10):
    return None if random.random() < pct else value


def _corrupt_email(email, pct=0.05):
    if random.random() < pct:
        return email.replace("@", "").replace(".", "")  # invalid format
    return email


def _corrupt_ssn(ssn, pct=0.08):
    if random.random() < pct:
        return "INVALID-SSN"
    return ssn


def create_database():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # --- CUSTOMERS (10,000 rows) ---
    c.execute("""CREATE TABLE CUSTOMERS (
        customer_id TEXT PRIMARY KEY,
        first_name TEXT, last_name TEXT,
        email TEXT, phone TEXT, ssn TEXT,
        dob TEXT, address TEXT, city TEXT, state TEXT,
        zip TEXT, country TEXT,
        credit_limit REAL, status TEXT,
        created_date TEXT, updated_date TEXT
    )""")

    customers = []
    used_ids = set()
    for i in range(10000):
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        cid = f"CUST-{i+1:06d}"
        # Inject ~5% duplicates
        if random.random() < 0.05 and used_ids:
            cid = random.choice(list(used_ids))
        else:
            used_ids.add(cid)
        email = _corrupt_email(_rand_email(first, last))
        customers.append((
            cid, first, last,
            _maybe_null(email, 0.12),
            _maybe_null(_rand_phone(), 0.08),
            _maybe_null(_corrupt_ssn(_rand_ssn()), 0.10),
            _maybe_null(_rand_date(1955, 2000), 0.05),
            f"{random.randint(100,9999)} {random.choice(['Main','Oak','Elm','Park'])} St",
            random.choice(CITIES),
            random.choice(STATES),
            f"{random.randint(10000,99999)}",
            "USA",
            _maybe_null(round(random.uniform(1000, 50000), 2), 0.07),
            random.choice(["ACTIVE","INACTIVE","SUSPENDED","PENDING"]),
            _rand_date(2015, 2022),
            _maybe_null(_rand_date(2022, 2024), 0.15),
        ))
    c.executemany("INSERT OR IGNORE INTO CUSTOMERS VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", customers)

    # --- ORDERS (50,000 rows) ---
    c.execute("""CREATE TABLE ORDERS (
        order_id TEXT PRIMARY KEY,
        customer_id TEXT, product_id TEXT,
        order_date TEXT, ship_date TEXT,
        amount REAL, quantity INTEGER,
        status TEXT, region TEXT,
        created_date TEXT
    )""")
    customer_ids = [f"CUST-{i+1:06d}" for i in range(10000)]
    product_ids = [f"PROD-{i+1:04d}" for i in range(500)]
    orders = []
    for i in range(50000):
        cid = random.choice(customer_ids)
        # Inject 3% bad FK
        if random.random() < 0.03:
            cid = f"CUST-INVALID-{i}"
        amt = round(random.uniform(-50, 5000), 2)  # some negatives = bad data
        if random.random() > 0.05:
            amt = abs(amt)
        orders.append((
            f"ORD-{i+1:07d}",
            cid, random.choice(product_ids),
            _rand_date(2020, 2024),
            _maybe_null(_rand_date(2020, 2024), 0.08),
            amt,
            random.randint(1, 100),
            random.choice(["PENDING","SHIPPED","DELIVERED","CANCELLED","RETURNED"]),
            random.choice(["NORTH","SOUTH","EAST","WEST","CENTRAL"]),
            _rand_date(2020, 2024),
        ))
    c.executemany("INSERT OR IGNORE INTO ORDERS VALUES (?,?,?,?,?,?,?,?,?,?)", orders)

    # --- PRODUCTS (500 rows) ---
    c.execute("""CREATE TABLE PRODUCTS (
        product_id TEXT PRIMARY KEY,
        name TEXT, category TEXT,
        price REAL, description TEXT,
        stock_qty INTEGER, sku TEXT,
        created_date TEXT
    )""")
    products = []
    prod_used = set()
    for i in range(500):
        pid = f"PROD-{i+1:04d}"
        if random.random() < 0.03 and prod_used:
            pid = random.choice(list(prod_used))
        else:
            prod_used.add(pid)
        cat = random.choice(CATEGORIES)
        products.append((
            pid,
            f"{cat} Item {i+1}",
            cat,
            _maybe_null(round(random.uniform(10, 5000), 2), 0.10),
            _maybe_null(f"High-quality {cat.lower()} product for enterprise use.", 0.05),
            random.randint(0, 10000),
            f"SKU-{random.randint(100000,999999)}",
            _rand_date(2015, 2022),
        ))
    c.executemany("INSERT OR IGNORE INTO PRODUCTS VALUES (?,?,?,?,?,?,?,?)", products)

    # --- EMPLOYEES (2,000 rows) ---
    c.execute("""CREATE TABLE EMPLOYEES (
        emp_id TEXT PRIMARY KEY,
        first_name TEXT, last_name TEXT,
        email TEXT, ssn TEXT,
        department TEXT, salary REAL,
        hire_date TEXT, manager_id TEXT, status TEXT
    )""")
    employees = []
    emp_ids = [f"EMP-{i+1:05d}" for i in range(2000)]
    for i in range(2000):
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        mgr = _maybe_null(random.choice(emp_ids), 0.10)
        if mgr and random.random() < 0.10:
            mgr = f"EMP-INVALID-{i}"  # orphaned FK
        employees.append((
            emp_ids[i], first, last,
            _maybe_null(_rand_email(first, last), 0.05),
            _maybe_null(_corrupt_ssn(_rand_ssn(), 0.08), 0.05),
            random.choice(DEPARTMENTS),
            _maybe_null(round(random.uniform(40000, 250000), 2), 0.05),
            _rand_date(2005, 2024),
            mgr,
            random.choice(["ACTIVE","INACTIVE","ON_LEAVE"]),
        ))
    c.executemany("INSERT OR IGNORE INTO EMPLOYEES VALUES (?,?,?,?,?,?,?,?,?,?)", employees)

    # --- FINANCIALS (20,000 rows) ---
    c.execute("""CREATE TABLE FINANCIALS (
        txn_id TEXT PRIMARY KEY,
        account_id TEXT, amount REAL,
        txn_date TEXT, txn_type TEXT,
        currency TEXT, description TEXT,
        created_date TEXT
    )""")
    financials = []
    txn_used = set()
    for i in range(20000):
        tid = f"TXN-{i+1:08d}"
        if random.random() < 0.02 and txn_used:
            tid = random.choice(list(txn_used))
        else:
            txn_used.add(tid)
        curr = random.choice(CURRENCIES)
        if random.random() < 0.03:
            curr = "INVALID"
        financials.append((
            tid,
            f"ACC-{random.randint(100000,999999)}",
            round(random.uniform(-100000, 100000), 2),
            _rand_date(2020, 2024),
            random.choice(TXN_TYPES),
            curr,
            _maybe_null(f"Payment for {random.choice(CATEGORIES).lower()} services", 0.05),
            _rand_date(2020, 2024),
        ))
    c.executemany("INSERT OR IGNORE INTO FINANCIALS VALUES (?,?,?,?,?,?,?,?)", financials)

    # --- INVENTORY (3,000 rows) ---
    c.execute("""CREATE TABLE INVENTORY (
        item_id TEXT PRIMARY KEY,
        product_id TEXT, warehouse TEXT,
        qty_on_hand INTEGER, reorder_level INTEGER,
        last_updated TEXT, unit_cost REAL
    )""")
    inventory = []
    for i in range(3000):
        pid = random.choice(product_ids)
        if random.random() < 0.05:
            pid = f"PROD-INVALID-{i}"
        inventory.append((
            f"INV-{i+1:06d}",
            pid,
            random.choice(WAREHOUSES),
            random.randint(0, 5000),
            random.randint(10, 500),
            _maybe_null(_rand_date(2023, 2024), 0.08),
            _maybe_null(round(random.uniform(1, 2000), 2), 0.06),
        ))
    c.executemany("INSERT OR IGNORE INTO INVENTORY VALUES (?,?,?,?,?,?,?)", inventory)

    # --- CONTRACTS (1,000 rows) ---
    c.execute("""CREATE TABLE CONTRACTS (
        contract_id TEXT PRIMARY KEY,
        customer_id TEXT,
        start_date TEXT, end_date TEXT,
        value REAL, status TEXT, terms_text TEXT,
        created_date TEXT
    )""")
    contracts = []
    terms_templates = [
        "This Master Service Agreement governs the provision of enterprise data pipeline services. Client agrees to maintain data sovereignty requirements per GDPR Article 17.",
        "The Software License Agreement grants non-exclusive rights to use the AI Ready Data platform. Annual subscription includes unlimited table migrations and 24/7 SLA support.",
        "Data Processing Agreement under CCPA and SOC 2 Type II compliance framework. All PII data handling subject to Microsoft Purview governance controls.",
        "Professional Services engagement for Oracle to OneLake migration. Fixed-fee project scope includes discovery, transformation, and governance certification.",
    ]
    for i in range(1000):
        cid = random.choice(customer_ids)
        if random.random() < 0.05:
            cid = f"CUST-INVALID-{i}"
        sd = _rand_date(2020, 2023)
        contracts.append((
            f"CTR-{i+1:05d}",
            cid, sd,
            _maybe_null(_rand_date(2024, 2026), 0.10),
            round(random.uniform(10000, 2000000), 2),
            random.choice(CONTRACT_STATUSES),
            random.choice(terms_templates),
            sd,
        ))
    c.executemany("INSERT OR IGNORE INTO CONTRACTS VALUES (?,?,?,?,?,?,?,?)", contracts)

    # --- RISK_EVENTS (5,000 rows) ---
    c.execute("""CREATE TABLE RISK_EVENTS (
        event_id TEXT PRIMARY KEY,
        customer_id TEXT, event_type TEXT,
        severity TEXT, event_date TEXT,
        description TEXT, resolved INTEGER,
        created_date TEXT
    )""")
    risk_events = []
    for i in range(5000):
        cid = random.choice(customer_ids)
        if random.random() < 0.08:
            cid = f"CUST-INVALID-{i}"
        risk_events.append((
            f"EVT-{i+1:07d}",
            cid,
            random.choice(EVENT_TYPES),
            random.choice(SEVERITIES),
            _rand_date(2020, 2024),
            f"Automated risk event: {random.choice(EVENT_TYPES).lower()} detected for account.",
            _maybe_null(random.randint(0, 1), 0.15),
            _rand_date(2020, 2024),
        ))
    c.executemany("INSERT OR IGNORE INTO RISK_EVENTS VALUES (?,?,?,?,?,?,?,?)", risk_events)

    conn.commit()
    conn.close()
    return DB_PATH


def get_table_stats():
    """Return row counts and column counts for all tables."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    tables = {
        "CUSTOMERS": {"display": "Customers", "cols": 16, "pii": ["email","ssn","dob","phone"], "domain": "CRM"},
        "ORDERS": {"display": "Orders", "cols": 10, "pii": [], "domain": "Sales"},
        "PRODUCTS": {"display": "Products", "cols": 8, "pii": [], "domain": "Catalog"},
        "EMPLOYEES": {"display": "Employees", "cols": 10, "pii": ["email","ssn"], "domain": "HR"},
        "FINANCIALS": {"display": "Financials", "cols": 8, "pii": [], "domain": "Finance"},
        "INVENTORY": {"display": "Inventory", "cols": 7, "pii": [], "domain": "Supply Chain"},
        "CONTRACTS": {"display": "Contracts", "cols": 8, "pii": [], "domain": "Legal"},
        "RISK_EVENTS": {"display": "Risk Events", "cols": 8, "pii": [], "domain": "Risk"},
    }
    result = {}
    for tbl, meta in tables.items():
        try:
            row = c.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()
            result[tbl] = {**meta, "rows": row[0]}
        except Exception:
            result[tbl] = {**meta, "rows": 0}
    conn.close()
    return result


# DQ issue counts per table (simulated from known injection rates)
TABLE_DQ_ISSUES = {
    "CUSTOMERS": {
        "null_pct": 12, "duplicate_pct": 5, "format_error_pct": 5,
        "fk_violation_pct": 0, "range_error_pct": 0, "stale_pct": 15, "volume_ok": True
    },
    "ORDERS": {
        "null_pct": 8, "duplicate_pct": 0, "format_error_pct": 0,
        "fk_violation_pct": 3, "range_error_pct": 5, "stale_pct": 0, "volume_ok": True
    },
    "PRODUCTS": {
        "null_pct": 10, "duplicate_pct": 3, "format_error_pct": 0,
        "fk_violation_pct": 0, "range_error_pct": 0, "stale_pct": 0, "volume_ok": True
    },
    "EMPLOYEES": {
        "null_pct": 5, "duplicate_pct": 0, "format_error_pct": 8,
        "fk_violation_pct": 10, "range_error_pct": 0, "stale_pct": 0, "volume_ok": True
    },
    "FINANCIALS": {
        "null_pct": 5, "duplicate_pct": 2, "format_error_pct": 3,
        "fk_violation_pct": 0, "range_error_pct": 0, "stale_pct": 0, "volume_ok": True
    },
    "INVENTORY": {
        "null_pct": 8, "duplicate_pct": 0, "format_error_pct": 0,
        "fk_violation_pct": 5, "range_error_pct": 0, "stale_pct": 8, "volume_ok": True
    },
    "CONTRACTS": {
        "null_pct": 10, "duplicate_pct": 0, "format_error_pct": 0,
        "fk_violation_pct": 5, "range_error_pct": 0, "stale_pct": 0, "volume_ok": True
    },
    "RISK_EVENTS": {
        "null_pct": 15, "duplicate_pct": 0, "format_error_pct": 0,
        "fk_violation_pct": 8, "range_error_pct": 0, "stale_pct": 0, "volume_ok": True
    },
}

# AI Readiness scores per layer
READINESS_SCORES = {
    "CUSTOMERS":  {"source": 48, "bronze": 52, "silver": 76, "gold": 91, "platinum": 96},
    "ORDERS":     {"source": 59, "bronze": 61, "silver": 80, "gold": 89, "platinum": 94},
    "PRODUCTS":   {"source": 67, "bronze": 71, "silver": 83, "gold": 92, "platinum": 97},
    "EMPLOYEES":  {"source": 51, "bronze": 55, "silver": 74, "gold": 88, "platinum": 95},
    "FINANCIALS": {"source": 64, "bronze": 68, "silver": 82, "gold": 90, "platinum": 96},
    "INVENTORY":  {"source": 70, "bronze": 74, "silver": 85, "gold": 93, "platinum": 97},
    "CONTRACTS":  {"source": 60, "bronze": 63, "silver": 78, "gold": 87, "platinum": 93},
    "RISK_EVENTS":{"source": 53, "bronze": 57, "silver": 75, "gold": 88, "platinum": 94},
}

SCORE_FACTORS = {
    "CUSTOMERS":  {"completeness":58,"uniqueness":72,"freshness":61,"cardinality":88,"referential":95},
    "ORDERS":     {"completeness":70,"uniqueness":95,"freshness":75,"cardinality":82,"referential":72},
    "PRODUCTS":   {"completeness":75,"uniqueness":90,"freshness":80,"cardinality":70,"referential":98},
    "EMPLOYEES":  {"completeness":80,"uniqueness":97,"freshness":72,"cardinality":65,"referential":55},
    "FINANCIALS": {"completeness":78,"uniqueness":88,"freshness":85,"cardinality":75,"referential":98},
    "INVENTORY":  {"completeness":82,"uniqueness":98,"freshness":68,"cardinality":72,"referential":78},
    "CONTRACTS":  {"completeness":68,"uniqueness":97,"freshness":74,"cardinality":60,"referential":80},
    "RISK_EVENTS":{"completeness":62,"uniqueness":98,"freshness":78,"cardinality":70,"referential":72},
}
