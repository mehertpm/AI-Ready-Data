# AI Ready Data — 9-Stage Pipeline POC

> Oracle Legacy Databases → Microsoft Fabric OneLake — fully automated, end-to-end

## Quick Start

```bash
./start.sh
# then open http://localhost:8000
```

Or manually:
```bash
cd backend
pip install -r requirements.txt
python main.py
```

## What the POC Demonstrates

### 9 Automated Stages
| # | Stage | What it does |
|---|-------|-------------|
| 1 | **Discovery** | Auto-connect Oracle, profile all 8 tables, detect PII (6 categories), build FK dependency graph |
| 2 | **Assessment** | Compute AI Readiness Score 0–100 per table (completeness/uniqueness/freshness/cardinality/referential) |
| 3 | **Architecture** | Auto-generate Bronze/Silver/Gold/Platinum medallion DDL + Fabric notebooks |
| 4 | **Extraction** | 8-way parallel ROWID-range load → Bronze, dead-letter quarantine, schema drift detection |
| 5 | **Transform** | Cleanse nulls, SCD2 deduplication, type normalization → Silver |
| 6 | **Data Quality** | 7 rule types (null/uniqueness/range/regex/referential/volume/freshness) → auto-quarantine → Gold |
| 7 | **Governance** | Microsoft Purview registration, column-level lineage, MIP sensitivity labels on PII |
| 8 | **AI Optimize** | Rolling window features, text chunking (RAG), ML feature vectors → Platinum |
| 9 | **Monitoring** | Live WebSocket dashboards, DQ trend alerts, schema drift detection |

### Bronze → Silver → Gold Progression
Each of the 8 Oracle tables moves through layers with rising AI Readiness Scores:

| Layer | Score Range | What changed |
|-------|-------------|-------------|
| Bronze | 52–74 | Raw data, nulls, duplicates, format errors intact |
| Silver | 74–85 | Nulls imputed, duplicates removed via SCD2, types normalized |
| Gold | 87–93 | DQ rules applied, failed records quarantined, aggregations ready |
| Platinum | 93–97 | ML features, embeddings, text chunks — fully AI-ready |

### Simulated Oracle Source (8 enterprise tables)
- `CUSTOMERS` — 10,000 rows with 12% null emails, 5% duplicates, SSN format errors
- `ORDERS` — 50,000 rows with FK violations, negative amounts
- `PRODUCTS` — 500 rows with null prices, duplicates
- `EMPLOYEES` — 2,000 rows with SSN format errors, orphaned manager FKs
- `FINANCIALS` — 20,000 rows with invalid currencies, duplicate transactions
- `INVENTORY` — 3,000 rows with stale timestamps, FK violations
- `CONTRACTS` — 1,000 rows with long-text for RAG chunking
- `RISK_EVENTS` — 5,000 rows with 15% null resolved flags

### 7 DQ Rule Types (Stage 6)
1. **Null Check** — threshold auto-set from column profiling
2. **Uniqueness** — PK and high-cardinality enforcement
3. **Range** — min/max from P1–P99 percentiles
4. **Regex Pattern** — email, phone, SSN, IP validation
5. **Referential** — FK integrity across all relationships
6. **Volume Anomaly** — row count vs historical baseline
7. **Freshness** — timestamp staleness from update pattern

## Stack
- **Backend**: Python FastAPI + WebSockets + SQLite (Oracle simulator)
- **Frontend**: React 18 (CDN) + single HTML file — no build step
- **No cloud credentials required** — fully local simulation

## Project Structure
```
ai-ready-data-poc/
├── backend/
│   ├── main.py            # FastAPI server + WebSocket
│   ├── oracle_sim.py      # SQLite Oracle simulator (8 tables, ~91K rows)
│   ├── pipeline.py        # 9-stage pipeline engine
│   └── requirements.txt
├── frontend/
│   └── index.html         # React dashboard (self-contained)
├── start.sh               # One-command launcher
└── README.md
```
