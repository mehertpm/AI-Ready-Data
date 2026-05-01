"""
Banking Pipeline Engine — 9-stage async pipeline with AML/KYC/PCI compliance scoring
"""
import asyncio, time, random
from bank_oracle_sim import (
    READINESS_SCORES, COMPLIANCE_SCORES, TABLE_DQ_ISSUES,
    SCORE_FACTORS, PHI_DETECTED, IMPROVEMENT_PATHS,
    PCI_IDENTIFIERS, AML_KYC_FIELDS, TABLE_ORDER, get_table_stats
)

STAGE_NAMES = [
    "Discovery",
    "Assessment",
    "Architecture",
    "Extraction",
    "Transform",
    "Data Quality",
    "Governance",
    "AI Optimize",
    "Monitoring",
]

BANK_DQ_RULES = {
    "kyc_completeness":        {"label": "KYC Completeness",          "description": "All required KYC fields populated per CIP requirements"},
    "aml_velocity_check":      {"label": "AML Velocity Check",        "description": "Transaction velocity within AML threshold rules (24h/7d/30d)"},
    "balance_integrity":       {"label": "Balance Integrity",         "description": "Account balances reconcile with transaction history"},
    "luhn_validity":           {"label": "Card Luhn Validity",        "description": "All card PANs pass Luhn algorithm check"},
    "routing_number_validity": {"label": "Routing Number Validity",   "description": "ABA routing numbers validated against Fed ACH directory"},
    "sar_timeliness":          {"label": "SAR Filing Timeliness",     "description": "Suspicious Activity Reports filed within 30-day FinCEN SLA"},
    "referential_integrity":   {"label": "Referential Integrity",     "description": "All foreign keys resolve to valid parent records"},
}

DQ_PASS_RATES = {
    "CUSTOMERS":     {"kyc_completeness": 0.80, "aml_velocity_check": 0.95, "balance_integrity": 1.00, "luhn_validity": 1.00, "routing_number_validity": 0.92, "sar_timeliness": 0.88, "referential_integrity": 0.91},
    "ACCOUNTS":      {"kyc_completeness": 0.95, "aml_velocity_check": 0.97, "balance_integrity": 0.95, "luhn_validity": 1.00, "routing_number_validity": 0.96, "sar_timeliness": 1.00, "referential_integrity": 0.98},
    "TRANSACTIONS":  {"kyc_completeness": 0.99, "aml_velocity_check": 0.92, "balance_integrity": 0.97, "luhn_validity": 1.00, "routing_number_validity": 0.98, "sar_timeliness": 0.90, "referential_integrity": 0.95},
    "CARDS":         {"kyc_completeness": 0.98, "aml_velocity_check": 1.00, "balance_integrity": 1.00, "luhn_validity": 0.95, "routing_number_validity": 1.00, "sar_timeliness": 1.00, "referential_integrity": 0.96},
    "LOANS":         {"kyc_completeness": 0.97, "aml_velocity_check": 1.00, "balance_integrity": 0.98, "luhn_validity": 1.00, "routing_number_validity": 0.99, "sar_timeliness": 1.00, "referential_integrity": 0.97},
    "BENEFICIARIES": {"kyc_completeness": 0.85, "aml_velocity_check": 0.94, "balance_integrity": 1.00, "luhn_validity": 1.00, "routing_number_validity": 0.88, "sar_timeliness": 0.96, "referential_integrity": 0.85},
    "RISK_EVENTS":   {"kyc_completeness": 0.90, "aml_velocity_check": 0.97, "balance_integrity": 1.00, "luhn_validity": 1.00, "routing_number_validity": 1.00, "sar_timeliness": 0.75, "referential_integrity": 0.94},
    "BRANCHES":      {"kyc_completeness": 1.00, "aml_velocity_check": 1.00, "balance_integrity": 0.95, "luhn_validity": 1.00, "routing_number_validity": 0.99, "sar_timeliness": 1.00, "referential_integrity": 0.92},
}


class BankPipelineEngine:
    def __init__(self, broadcast_fn):
        self._bc = broadcast_fn
        self._stats = get_table_stats()

    async def _log(self, msg, level="INFO"):
        await self._bc({"type": "log", "level": level, "message": msg})
        await asyncio.sleep(0.03)

    async def _stage(self, n, name):
        await self._bc({"type": "stage_start", "stage": n, "name": name})
        await self._log(f"━━━ Stage {n+1}: {name} ━━━")

    async def _done(self, n):
        await self._bc({"type": "stage_complete", "stage": n})

    async def _tbl(self, tbl, **kw):
        await self._bc({"type": "table_update", "table": tbl, **kw})

    async def run(self):
        t0 = time.time()
        await self._stage_1_discovery()
        await self._stage_2_assessment()
        await self._stage_3_architecture()
        await self._stage_4_extraction()
        await self._stage_5_transform()
        await self._stage_6_dq()
        await self._stage_7_governance()
        await self._stage_8_ai_optimize()
        await self._stage_9_monitoring()
        duration = round(time.time() - t0, 1)
        await self._bc({"type": "pipeline_complete", "duration": duration})
        await self._log(f"Pipeline complete in {duration}s — Banking data lake ready", "SUCCESS")

    # ── Stage 1: Discovery ────────────────────────────────────────────────────
    async def _stage_1_discovery(self):
        await self._stage(0, "Discovery")
        await self._log("Connecting to core banking system (FIS/Fiserv simulation)...")
        await asyncio.sleep(0.4)
        await self._log("Authenticating with service account PIPELINE_SVC...")
        await asyncio.sleep(0.3)
        await self._log(f"Discovered {len(TABLE_ORDER)} tables across Banking schema")
        await asyncio.sleep(0.2)

        for tbl in TABLE_ORDER:
            s = self._stats[tbl]
            await self._log(f"  {tbl}: {s['rows']:,} rows | Domain: {s['domain']} | Reg: {s['regulatory']}")
            await self._tbl(tbl,
                layer="source",
                score=READINESS_SCORES[tbl]["source"],
                compliance_score=COMPLIANCE_SCORES[tbl]["source"],
                rows_processed=s["rows"],
                domain=s["domain"],
                regulatory=s["regulatory"],
                display=s["display"],
            )
            await asyncio.sleep(0.15)

        await self._log("Schema profiling complete — column stats, nullability, cardinality captured")
        await asyncio.sleep(0.3)
        await self._log(f"Total rows discovered: {sum(s['rows'] for s in self._stats.values()):,}")
        await self._done(0)

    # ── Stage 2: Assessment ───────────────────────────────────────────────────
    async def _stage_2_assessment(self):
        await self._stage(1, "Assessment")
        await self._log("Computing AI Readiness Scores and Compliance Gap Analysis...")
        await asyncio.sleep(0.4)

        for tbl in TABLE_ORDER:
            sc = READINESS_SCORES[tbl]["source"]
            cc = COMPLIANCE_SCORES[tbl]["source"]
            factors = SCORE_FACTORS[tbl]
            issues = TABLE_DQ_ISSUES[tbl]

            await self._log(f"  {tbl} — AI Score: {sc}/100 | Compliance: {cc}/100")
            for issue, desc in list(issues.items())[:2]:
                await self._log(f"    ⚠ {desc}", "WARN")
            await self._log(f"    Score factors: {', '.join(f'{k}({v}%)' for k,v in list(factors.items())[:3])}")
            await self._tbl(tbl, score=sc, compliance_score=cc, dq_issues=issues, score_factors=factors)
            await asyncio.sleep(0.2)

        await self._log("Regulatory framework mapping: BSA/AML, PCI-DSS, GLBA, HMDA, CRA, FDIC")
        await asyncio.sleep(0.3)
        await self._log("Assessment complete — 6 critical compliance gaps identified across 3 tables", "WARN")
        await self._done(1)

    # ── Stage 3: Architecture ─────────────────────────────────────────────────
    async def _stage_3_architecture(self):
        await self._stage(2, "Architecture")
        await self._log("Generating Medallion DDL for Banking Data Lakehouse...")
        await asyncio.sleep(0.4)

        layers = [
            ("Bronze", "Raw ingestion — preserve source fidelity, add audit columns"),
            ("Silver", "Cleansed — KYC validated, formats standardized, PAN tokenized"),
            ("Gold",   "Curated — AML-scored, OFAC-screened, referential integrity enforced"),
            ("Platinum","AI-ready — feature vectors, risk scores, regulatory reports"),
        ]
        for layer, desc in layers:
            await self._log(f"  {layer} Layer: {desc}")
            await asyncio.sleep(0.2)

        await self._log("DDL generated: 8 Bronze tables, 8 Silver views, 8 Gold aggregates, 4 Platinum feature stores")
        await asyncio.sleep(0.3)
        await self._log("Partition strategy: TRANSACTIONS → partition by transaction_date (monthly)")
        await self._log("PCI-DSS Tokenization vault schema generated for CARDS and TRANSACTIONS")
        await self._log("AML feature store schema: customer_risk_profile, txn_velocity_windows, network_graph_edges")
        await asyncio.sleep(0.2)
        await self._done(2)

    # ── Stage 4: Extraction ───────────────────────────────────────────────────
    async def _stage_4_extraction(self):
        await self._stage(3, "Extraction")
        await self._log("8-way parallel extraction from core banking tables (semaphore=4)...")
        await asyncio.sleep(0.3)

        sem = asyncio.Semaphore(4)

        async def extract(tbl):
            async with sem:
                rows = self._stats[tbl]["rows"]
                await self._log(f"  Extracting {tbl} — {rows:,} rows via change data capture...")
                await asyncio.sleep(random.uniform(0.2, 0.5))
                await self._tbl(tbl, layer="bronze", score=READINESS_SCORES[tbl]["bronze"],
                                compliance_score=COMPLIANCE_SCORES[tbl]["bronze"],
                                rows_processed=rows)
                await self._log(f"  ✓ {tbl} → Bronze ({rows:,} rows, watermark updated)")

        await asyncio.gather(*[extract(t) for t in TABLE_ORDER])
        total = sum(s["rows"] for s in self._stats.values())
        await self._log(f"Extraction complete: {total:,} rows loaded to Bronze layer")
        await asyncio.sleep(0.2)
        await self._done(3)

    # ── Stage 5: Transform ────────────────────────────────────────────────────
    async def _stage_5_transform(self):
        await self._stage(4, "Transform")
        await self._log("Applying Silver-layer transformations...")
        await asyncio.sleep(0.3)

        transforms = [
            ("CUSTOMERS",     "De-duplicating on name+DOB+SSN-last4; KYC date normalization; email validation RFC 5322"),
            ("ACCOUNTS",      "Reconciling negative balances; populating missing interest rates from product master; dormancy flagging"),
            ("TRANSACTIONS",  "Deduplicating on txn_id+account+amount+date composite; orphan removal; normalizing to ISO 8583"),
            ("CARDS",         "PAN tokenization (PCI-DSS Req 3.4); updating expired card statuses; Luhn validation"),
            ("LOANS",         "LTV recalculation with current appraisals; delinquency day computation; rate outlier flagging"),
            ("BENEFICIARIES", "BIC/SWIFT validation against SWIFT directory; routing number Fed validation; KYC linkage"),
            ("RISK_EVENTS",   "SLA breach flagging; narrative enrichment; risk score imputation via rule engine"),
            ("BRANCHES",      "Inactive branch suspension flags; address normalization; license expiry alerts"),
        ]

        for tbl, desc in transforms:
            rows = self._stats[tbl]["rows"]
            quarantined = int(rows * random.uniform(0.02, 0.08))
            await self._log(f"  {tbl}: {desc}")
            await self._log(f"    → {rows - quarantined:,} rows promoted to Silver | {quarantined:,} quarantined")
            await self._tbl(tbl, layer="silver", score=READINESS_SCORES[tbl]["silver"],
                            compliance_score=COMPLIANCE_SCORES[tbl]["silver"],
                            rows_quarantined=quarantined)
            await asyncio.sleep(random.uniform(0.2, 0.4))

        await self._log("PAN tokenization complete — 6,000 card numbers replaced with vault tokens")
        await self._log("SCD2 applied to CUSTOMERS and ACCOUNTS — 847 history records created")
        await asyncio.sleep(0.2)
        await self._done(4)

    # ── Stage 6: Data Quality ─────────────────────────────────────────────────
    async def _stage_6_dq(self):
        await self._stage(5, "Data Quality")
        await self._log("Running 7 banking DQ rules across all tables...")
        await asyncio.sleep(0.4)

        lineage = {}
        for tbl in TABLE_ORDER:
            await self._log(f"  DQ rules for {tbl}:")
            rules = {}
            for rule_id, rule_info in BANK_DQ_RULES.items():
                pass_rate = DQ_PASS_RATES[tbl][rule_id]
                rows = self._stats[tbl]["rows"]
                passed = int(rows * pass_rate)
                failed = rows - passed
                status = "PASS" if pass_rate >= 0.95 else ("WARN" if pass_rate >= 0.80 else "FAIL")
                rules[rule_id] = {
                    "label": rule_info["label"],
                    "description": rule_info["description"],
                    "status": status,
                    "pass_rate": round(pass_rate * 100, 1),
                    "failed_rows": failed,
                }
                icon = "✓" if status == "PASS" else ("⚠" if status == "WARN" else "✗")
                await self._log(f"    {icon} {rule_info['label']}: {pass_rate*100:.0f}% ({failed:,} failures)")
                await asyncio.sleep(0.04)

            await self._bc({"type": "dq_result", "table": tbl, "rules": rules})
            await self._tbl(tbl, layer="gold", score=READINESS_SCORES[tbl]["gold"],
                            compliance_score=COMPLIANCE_SCORES[tbl]["gold"])

            lineage[tbl] = {
                "source": f"core_banking.{tbl}",
                "bronze": f"bronze.bank_{tbl.lower()}",
                "silver": f"silver.bank_{tbl.lower()}_cleansed",
                "gold":   f"gold.bank_{tbl.lower()}_certified",
            }
            await asyncio.sleep(0.15)

        await self._bc({"type": "lineage_update", "lineage": lineage})
        await self._log("DQ complete — TRANSACTIONS and RISK_EVENTS flagged for additional remediation", "WARN")
        await self._done(5)

    # ── Stage 7: Governance ───────────────────────────────────────────────────
    async def _stage_7_governance(self):
        await self._stage(6, "Governance")
        await self._log("Applying regulatory governance framework...")
        await asyncio.sleep(0.4)

        await self._log("  BSA/AML program: tagging all transaction flows with AML monitoring profiles")
        await asyncio.sleep(0.2)
        await self._log("  PCI-DSS Scope Reduction: tokenization confirmed, CHD environment boundary defined")
        await asyncio.sleep(0.2)
        await self._log("  OFAC/SDN screening: 8,842 beneficiaries re-screened | 2 potential hits flagged")
        await asyncio.sleep(0.2)
        await self._log("  GLBA data classification: PII fields tagged across CUSTOMERS, LOANS, CARDS")
        await asyncio.sleep(0.2)
        await self._log("  SOX controls: financial account data lineage documented for audit trail")
        await asyncio.sleep(0.2)

        pci = {id_: True for id_ in PCI_IDENTIFIERS}
        kyc = {field: True for field in AML_KYC_FIELDS}
        await self._bc({
            "type": "compliance_summary",
            "pci_identifiers": pci,
            "aml_kyc_fields": kyc,
            "improvement_paths": IMPROVEMENT_PATHS,
        })

        await self._log("  CRA assessment area mapping: branches linked to FFIEC census tracts")
        await asyncio.sleep(0.2)
        await self._log("  Governance catalog updated: 47 data assets registered | 312 lineage edges")
        await self._done(6)

    # ── Stage 8: AI Optimize ──────────────────────────────────────────────────
    async def _stage_8_ai_optimize(self):
        await self._stage(7, "AI Optimize")
        await self._log("Generating ML feature stores and Platinum promotions...")
        await asyncio.sleep(0.4)

        features = [
            ("CUSTOMERS",    "customer_risk_360",    "KYC completeness score, PEP flag, OFAC hit count, CDD tier, account vintage"),
            ("ACCOUNTS",     "account_health_score",  "Balance volatility, dormancy risk, product penetration, lifetime value"),
            ("TRANSACTIONS", "txn_aml_features",      "Velocity (1d/7d/30d), structuring score, cross-border ratio, round-amount flag"),
            ("CARDS",        "card_fraud_features",   "Geolocation anomaly score, merchant diversity, velocity, CNP ratio"),
            ("LOANS",        "pd_lgd_ead_features",   "Probability of Default, Loss Given Default, Exposure at Default (Basel III)"),
            ("BENEFICIARIES","bene_risk_score",       "Jurisdiction risk, verification status, OFAC proximity score, network centrality"),
            ("RISK_EVENTS",  "sar_quality_score",     "Alert accuracy, false positive rate, narrative completeness, typology coverage"),
            ("BRANCHES",     "branch_cra_score",      "LMI loan percentage, assessment area coverage, deposit-to-loan ratio"),
        ]

        for tbl, store, feats in features:
            await self._log(f"  {tbl} → Feature Store '{store}'")
            await self._log(f"    Features: {feats}")
            await self._tbl(tbl, layer="platinum", score=READINESS_SCORES[tbl]["platinum"],
                            compliance_score=COMPLIANCE_SCORES[tbl]["platinum"],
                            feature_store=store)
            await asyncio.sleep(random.uniform(0.2, 0.35))

        await self._log("Fraud model inference: 100K transactions scored in 4.2s (avg 0.04ms/txn)")
        await asyncio.sleep(0.2)
        await self._log("CECL reserve model: 3,000 loans scored for expected credit loss")
        await asyncio.sleep(0.2)
        await self._log("AML network graph: 12,847 nodes | 47,293 edges | 14 suspicious clusters detected")
        await self._done(7)

    # ── Stage 9: Monitoring ───────────────────────────────────────────────────
    async def _stage_9_monitoring(self):
        await self._stage(8, "Monitoring")
        await self._log("Activating regulatory monitoring and alerting systems...")
        await asyncio.sleep(0.4)

        monitors = [
            "CTR filing trigger: transactions ≥ $10,000 → auto-queue for FinCEN 112 form",
            "SAR SLA monitor: open alerts >25 days → escalation notification",
            "PCI-DSS: CHD access log anomaly detection (after-hours, bulk extract)",
            "FDIC insurance: daily deposit aggregation per customer per category",
            "Reg D: savings account withdrawal counter (6/month limit) — auto-freeze trigger",
            "OFAC: real-time screening on all new beneficiary additions",
            "CECL: monthly reserve recalculation job scheduled",
            "Data quality SLA: score degradation alert if any table drops below 80",
        ]
        for m in monitors:
            await self._log(f"  ✓ {m}")
            await asyncio.sleep(0.15)

        await self._log("Monitoring active — 8 regulatory monitors + 24 DQ checks running")
        await self._done(8)
