"""
9-Stage AI Ready Data Pipeline Engine.
Orchestrates each stage asynchronously, broadcasting WebSocket events
as tables progress from Oracle source → Bronze → Silver → Gold → Platinum.
"""
import asyncio
import time
import random
from oracle_sim import (
    get_table_stats, TABLE_DQ_ISSUES, READINESS_SCORES, SCORE_FACTORS
)

random.seed(99)

TABLE_ORDER = ["CUSTOMERS","ORDERS","PRODUCTS","EMPLOYEES",
               "FINANCIALS","INVENTORY","CONTRACTS","RISK_EVENTS"]

STAGE_NAMES = [
    "Discovery", "Assessment", "Architecture", "Extraction",
    "Transform", "Data Quality", "Governance", "AI Optimize", "Monitoring"
]


class PipelineEngine:
    def __init__(self, broadcast_fn):
        self.broadcast = broadcast_fn
        self.start_time = None
        self.table_stats = {}
        self.state = {
            "status": "running",
            "current_stage": 0,
            "stages_complete": [],
            "tables": {},
            "dq_results": {},
            "lineage": {},
            "quarantine": {},
            "ddl": {},
            "governance": {},
            "ai_features": {},
        }

    async def _log(self, level, message, stage=None):
        await self.broadcast({
            "type": "log",
            "level": level,
            "message": message,
            "stage": stage,
            "ts": time.strftime("%H:%M:%S"),
        })

    async def _stage_start(self, stage_num, name):
        self.state["current_stage"] = stage_num
        await self.broadcast({
            "type": "stage_start",
            "stage": stage_num,
            "name": name,
        })
        await self._log("info", f"▶  Stage {stage_num}: {name} — started", stage_num)

    async def _stage_progress(self, stage_num, progress, message):
        await self.broadcast({
            "type": "stage_progress",
            "stage": stage_num,
            "progress": progress,
            "message": message,
        })

    async def _stage_complete(self, stage_num, name, duration):
        self.state["stages_complete"].append(stage_num)
        await self.broadcast({
            "type": "stage_complete",
            "stage": stage_num,
            "name": name,
            "duration": round(duration, 2),
        })
        await self._log("success", f"✔  Stage {stage_num}: {name} — complete ({duration:.1f}s)", stage_num)

    async def _table_update(self, table, layer, score, rows_processed=None,
                             rows_quarantined=0, extra=None):
        self.state["tables"][table] = {
            "layer": layer,
            "score": score,
            "rows_processed": rows_processed or self.table_stats.get(table, {}).get("rows", 0),
            "rows_quarantined": rows_quarantined,
            **(extra or {}),
        }
        await self.broadcast({
            "type": "table_update",
            "table": table,
            "layer": layer,
            "score": score,
            "rows_processed": rows_processed or self.table_stats.get(table, {}).get("rows", 0),
            "rows_quarantined": rows_quarantined,
            **(extra or {}),
        })

    # ── Stage 1: Discovery ────────────────────────────────────────────────────
    async def stage_discovery(self):
        t0 = time.time()
        await self._stage_start(1, "Discovery")

        await self._log("info", "  Connecting to Oracle 19c at oracle-uat.corp.internal:1521/ERPROD", 1)
        await asyncio.sleep(0.4)
        await self._stage_progress(1, 5, "Connected — authenticating as PIPELINE_SVC_USER")
        await asyncio.sleep(0.3)
        await self._stage_progress(1, 12, "Authenticated — scanning schema ERP_PROD")

        self.table_stats = get_table_stats()
        total = len(TABLE_ORDER)

        for idx, tbl in enumerate(TABLE_ORDER):
            meta = self.table_stats.get(tbl, {})
            pct = 15 + int((idx / total) * 55)
            await self._stage_progress(pct, pct, f"Profiling {tbl}...")
            await self._log("info", f"  → {tbl}: {meta.get('rows',0):,} rows · "
                                    f"{meta.get('cols',0)} cols · domain: {meta.get('domain','?')} "
                                    f"· PII: {meta.get('pii',[])}", 1)
            await asyncio.sleep(0.22)

            # Mark as source layer
            score = READINESS_SCORES[tbl]["source"]
            await self._table_update(tbl, "source", score)

        await self._stage_progress(1, 75, "Building FK dependency graph...")
        await asyncio.sleep(0.3)
        await self._log("info", "  FK graph: ORDERS→CUSTOMERS, INVENTORY→PRODUCTS, CONTRACTS→CUSTOMERS, RISK_EVENTS→CUSTOMERS, EMPLOYEES→EMPLOYEES(mgr)", 1)
        await self._stage_progress(1, 88, "Detecting PII columns...")
        await asyncio.sleep(0.25)
        pii_cols = ["CUSTOMERS.email","CUSTOMERS.ssn","CUSTOMERS.dob","CUSTOMERS.phone",
                    "EMPLOYEES.email","EMPLOYEES.ssn"]
        await self._log("info", f"  PII detected ({len(pii_cols)} columns): {', '.join(pii_cols)}", 1)
        await self._stage_progress(1, 100, "Discovery complete")
        await self._stage_complete(1, "Discovery", time.time() - t0)
        await asyncio.sleep(0.3)

    # ── Stage 2: Assessment ───────────────────────────────────────────────────
    async def stage_assessment(self):
        t0 = time.time()
        await self._stage_start(2, "Assessment")
        await self._log("info", "  Computing AI Readiness Scores (0–100) per table", 2)
        await asyncio.sleep(0.2)

        for idx, tbl in enumerate(TABLE_ORDER):
            pct = 5 + int((idx / len(TABLE_ORDER)) * 90)
            await self._stage_progress(2, pct, f"Scoring {tbl}...")
            factors = SCORE_FACTORS[tbl]
            score = int(
                factors["completeness"] * 0.30 +
                factors["uniqueness"] * 0.20 +
                factors["freshness"] * 0.20 +
                factors["cardinality"] * 0.15 +
                factors["referential"] * 0.15
            )
            issues = TABLE_DQ_ISSUES[tbl]
            await self._log("info",
                f"  {tbl}: Score={score} | Completeness={factors['completeness']} "
                f"Uniqueness={factors['uniqueness']} Freshness={factors['freshness']} "
                f"Cardinality={factors['cardinality']} Referential={factors['referential']} "
                f"| Nulls={issues['null_pct']}% Dups={issues['duplicate_pct']}%", 2)
            await self._table_update(tbl, "source", score, extra={"factors": factors})
            await asyncio.sleep(0.18)

        await self._stage_progress(2, 100, "Assessment complete — 8 tables scored")
        await self._stage_complete(2, "Assessment", time.time() - t0)
        await asyncio.sleep(0.3)

    # ── Stage 3: Architecture ─────────────────────────────────────────────────
    async def stage_architecture(self):
        t0 = time.time()
        await self._stage_start(3, "Architecture")

        layers = ["Bronze (Raw Landing)", "Silver (Cleansed/SCD2)",
                  "Gold (Aggregated/Business Rules)", "Platinum (AI-Ready/ML Features)"]
        for idx, layer in enumerate(layers):
            pct = 10 + idx * 20
            await self._stage_progress(3, pct, f"Generating {layer} DDL...")
            await self._log("info", f"  Generating {layer} DDL for 8 tables...", 3)
            await asyncio.sleep(0.25)

        await self._stage_progress(3, 85, "Generating Fabric notebook templates...")
        await self._log("info", "  Fabric notebooks: 8 Bronze ingestion + 8 Silver transform + 8 Gold aggregation notebooks auto-generated", 3)
        await asyncio.sleep(0.2)
        await self._log("info", "  DDL stored: ./output/ddl/ — ready for Fabric Lakehouse deployment", 3)

        self.state["ddl"] = {tbl: {
            "bronze": f"CREATE TABLE bronze.{tbl.lower()} (...) USING DELTA PARTITIONED BY (_load_date);",
            "silver": f"CREATE TABLE silver.{tbl.lower()} (...) USING DELTA ZORDER BY (id);",
            "gold":   f"CREATE TABLE gold.{tbl.lower()} (...) USING DELTA;",
        } for tbl in TABLE_ORDER}

        await self._stage_progress(3, 100, "Architecture complete")
        await self._stage_complete(3, "Architecture", time.time() - t0)
        await asyncio.sleep(0.3)

    # ── Stage 4: Extraction ───────────────────────────────────────────────────
    async def stage_extraction(self):
        t0 = time.time()
        await self._stage_start(4, "Extraction")
        await self._log("info", "  Launching 8-way parallel extraction (ROWID-range partitioning)", 4)
        await asyncio.sleep(0.2)

        # Simulate 8 parallel workers loading in chunks
        tasks = []
        semaphore = asyncio.Semaphore(8)

        async def load_table(tbl, idx):
            async with semaphore:
                rows = self.table_stats.get(tbl, {}).get("rows", 0)
                chunks = max(1, rows // 15000)
                score = READINESS_SCORES[tbl]["bronze"]
                dead_letter = int(rows * (TABLE_DQ_ISSUES[tbl]["null_pct"] / 100) * 0.1)

                for chunk in range(chunks):
                    pct = 5 + int(((idx * chunks + chunk) / (len(TABLE_ORDER) * chunks)) * 88)
                    rows_done = min((chunk + 1) * 15000, rows)
                    await self._stage_progress(4, pct, f"[Worker-{idx+1}] {tbl}: chunk {chunk+1}/{chunks}")
                    await self._log("info",
                        f"  [W-{idx+1}] {tbl}: {rows_done:,}/{rows:,} rows → Bronze "
                        f"(dead-letter: {dead_letter})", 4)
                    await asyncio.sleep(0.15 + random.uniform(0, 0.1))

                await self._table_update(tbl, "bronze", score,
                                          rows_processed=rows, rows_quarantined=dead_letter)
                await self._log("success",
                    f"  ✔ {tbl} → Bronze complete: {rows:,} rows loaded", 4)

        for idx, tbl in enumerate(TABLE_ORDER):
            tasks.append(load_table(tbl, idx))

        await asyncio.gather(*tasks)
        await self._stage_progress(4, 100, "All 8 tables loaded to Bronze")
        await self._stage_complete(4, "Extraction", time.time() - t0)
        await asyncio.sleep(0.3)

    # ── Stage 5: Transform ────────────────────────────────────────────────────
    async def stage_transform(self):
        t0 = time.time()
        await self._stage_start(5, "Transform")
        await self._log("info", "  Running Bronze→Silver transforms: cleanse, dedupe, SCD2, type normalization", 5)
        await asyncio.sleep(0.2)

        for idx, tbl in enumerate(TABLE_ORDER):
            pct = 5 + int((idx / len(TABLE_ORDER)) * 90)
            issues = TABLE_DQ_ISSUES[tbl]
            rows = self.table_stats.get(tbl, {}).get("rows", 0)
            dupes_removed = int(rows * issues["duplicate_pct"] / 100)
            nulls_imputed = int(rows * issues["null_pct"] / 100)
            formats_fixed = int(rows * issues["format_error_pct"] / 100)

            await self._stage_progress(5, pct, f"Transforming {tbl} → Silver")
            await self._log("info",
                f"  {tbl}: null-imputation ({nulls_imputed:,}) · "
                f"dedupe/SCD2 ({dupes_removed:,} dups removed) · "
                f"format-fix ({formats_fixed:,}) · type-cast applied", 5)
            await asyncio.sleep(0.3)

            silver_rows = rows - dupes_removed
            score = READINESS_SCORES[tbl]["silver"]
            await self._table_update(tbl, "silver", score,
                                      rows_processed=silver_rows, rows_quarantined=0)
            await self._log("success", f"  ✔ {tbl} → Silver: {silver_rows:,} rows", 5)

        await self._stage_progress(5, 100, "All tables promoted to Silver")
        await self._stage_complete(5, "Transform", time.time() - t0)
        await asyncio.sleep(0.3)

    # ── Stage 6: Data Quality ─────────────────────────────────────────────────
    async def stage_data_quality(self):
        t0 = time.time()
        await self._stage_start(6, "Data Quality")
        await self._log("info", "  Applying 7 DQ rule types across all Silver tables", 6)
        await asyncio.sleep(0.2)

        dq_rules = [
            ("null_check",     "Null Check",        "Null % threshold vs column profile"),
            ("uniqueness",     "Uniqueness",        "PK and high-cardinality enforcement"),
            ("range",          "Range Check",       "Min/max from P1–P99 percentiles"),
            ("regex_pattern",  "Regex Pattern",     "Email, phone, SSN, IP format validation"),
            ("referential",    "Referential",       "FK integrity across profiled relationships"),
            ("volume",         "Volume Anomaly",    "Row count vs historical baseline"),
            ("freshness",      "Freshness",         "Timestamp staleness from update patterns"),
        ]

        self.state["dq_results"] = {}

        for idx, tbl in enumerate(TABLE_ORDER):
            pct = 5 + int((idx / len(TABLE_ORDER)) * 88)
            await self._stage_progress(6, pct, f"Running DQ rules on {tbl}")
            rows = self.table_stats.get(tbl, {}).get("rows", 0)
            issues = TABLE_DQ_ISSUES[tbl]
            tbl_results = {}
            quarantined = 0

            for rule_id, rule_name, rule_desc in dq_rules:
                issue_pct = {
                    "null_check": issues["null_pct"],
                    "uniqueness": issues["duplicate_pct"],
                    "range": issues["range_error_pct"],
                    "regex_pattern": issues["format_error_pct"],
                    "referential": issues["fk_violation_pct"],
                    "volume": 0,
                    "freshness": issues["stale_pct"],
                }.get(rule_id, 0)

                failed = int(rows * issue_pct / 100)
                passed = rows - failed
                tbl_results[rule_id] = {
                    "name": rule_name, "desc": rule_desc,
                    "passed": passed, "failed": failed,
                    "pass_rate": round(100 * passed / max(rows, 1), 1),
                }
                quarantined += failed

                if failed > 0:
                    await self._log("warn",
                        f"  [{tbl}] {rule_name}: {failed:,} failed → quarantined", 6)
                else:
                    await self._log("info",
                        f"  [{tbl}] {rule_name}: PASS (100%)", 6)

            self.state["dq_results"][tbl] = tbl_results
            await self.broadcast({
                "type": "dq_result",
                "table": tbl,
                "rules": tbl_results,
                "quarantined": quarantined,
            })

            score = READINESS_SCORES[tbl]["gold"]
            gold_rows = max(0, rows - quarantined)
            await self._table_update(tbl, "gold", score,
                                      rows_processed=gold_rows, rows_quarantined=quarantined)
            await self._log("success",
                f"  ✔ {tbl} → Gold: {gold_rows:,} clean rows · {quarantined:,} quarantined", 6)
            await asyncio.sleep(0.28)

        await self._stage_progress(6, 100, "DQ complete — all tables at Gold")
        await self._stage_complete(6, "Data Quality", time.time() - t0)
        await asyncio.sleep(0.3)

    # ── Stage 7: Governance ───────────────────────────────────────────────────
    async def stage_governance(self):
        t0 = time.time()
        await self._stage_start(7, "Governance")
        await self._log("info", "  Registering assets in Microsoft Purview catalog", 7)
        await asyncio.sleep(0.3)

        lineage = {}
        for idx, tbl in enumerate(TABLE_ORDER):
            pct = 5 + int((idx / len(TABLE_ORDER)) * 70)
            await self._stage_progress(7, pct, f"Registering {tbl} in Purview")
            meta = self.table_stats.get(tbl, {})
            pii_cols = meta.get("pii", [])

            await self._log("info",
                f"  {tbl}: Purview catalog registered · lineage Source→Bronze→Silver→Gold tracked", 7)

            if pii_cols:
                await self._log("warn",
                    f"  {tbl}: MIP sensitivity labels applied to PII columns: {pii_cols}", 7)

            lineage[tbl] = {
                "nodes": ["Oracle Source", f"Bronze/{tbl}", f"Silver/{tbl}", f"Gold/{tbl}"],
                "edges": [
                    {"from": "Oracle Source", "to": f"Bronze/{tbl}", "transform": "8-way parallel extract"},
                    {"from": f"Bronze/{tbl}", "to": f"Silver/{tbl}", "transform": "Cleanse+SCD2"},
                    {"from": f"Silver/{tbl}", "to": f"Gold/{tbl}", "transform": "DQ+Aggregate"},
                ],
                "pii_columns": pii_cols,
            }
            self.state["lineage"][tbl] = lineage[tbl]
            await asyncio.sleep(0.2)

        await self._stage_progress(7, 90, "Schema drift detection enabled")
        await self._log("info", "  Schema drift monitoring enabled — downstream impact analysis active", 7)
        await self._stage_progress(7, 100, "Governance complete")
        await self.broadcast({"type": "lineage_update", "lineage": self.state["lineage"]})
        await self._stage_complete(7, "Governance", time.time() - t0)
        await asyncio.sleep(0.3)

    # ── Stage 8: AI Optimize ──────────────────────────────────────────────────
    async def stage_ai_optimize(self):
        t0 = time.time()
        await self._stage_start(8, "AI Optimize")
        await self._log("info", "  Generating AI-ready features: rolling windows, embeddings, text chunking", 8)
        await asyncio.sleep(0.2)

        for idx, tbl in enumerate(TABLE_ORDER):
            pct = 5 + int((idx / len(TABLE_ORDER)) * 88)
            rows = self.table_stats.get(tbl, {}).get("rows", 0)
            await self._stage_progress(8, pct, f"AI features for {tbl}")

            features = []
            if tbl in ("ORDERS", "FINANCIALS"):
                features.append("rolling_7d_avg · rolling_30d_sum · lag_features")
            if tbl == "CUSTOMERS":
                features.append("customer_ltv_score · churn_probability · segment_embedding")
            if tbl == "CONTRACTS":
                features.append(f"text chunked: {rows} docs → ~{rows * 3} chunks (512 tok/chunk) · LLM-ready")
            if tbl == "RISK_EVENTS":
                features.append("risk_score_vector · severity_embedding · temporal_pattern")
            if not features:
                features = ["feature_vector_128d · normalized_numeric_cols"]

            await self._log("info", f"  {tbl}: {' | '.join(features)}", 8)
            score = READINESS_SCORES[tbl]["platinum"]
            await self._table_update(tbl, "platinum", score, rows_processed=rows)
            await asyncio.sleep(0.22)

        await self._stage_progress(8, 100, "AI features complete — all tables at Platinum")
        await self._stage_complete(8, "AI Optimize", time.time() - t0)
        await asyncio.sleep(0.3)

    # ── Stage 9: Monitoring ───────────────────────────────────────────────────
    async def stage_monitoring(self):
        t0 = time.time()
        await self._stage_start(9, "Monitoring")
        await self._log("info", "  Initializing live WebSocket monitoring dashboards", 9)
        await asyncio.sleep(0.3)

        await self._stage_progress(9, 20, "Configuring DQ trend alerts")
        await self._log("info", "  DQ alerts: null_pct > 5% · volume anomaly > 3σ · freshness > 24h", 9)
        await asyncio.sleep(0.2)

        await self._stage_progress(9, 50, "Publishing metrics to monitoring endpoint")
        await self._log("info", "  Metrics endpoint: /api/metrics — Grafana-compatible", 9)
        await asyncio.sleep(0.2)

        await self._stage_progress(9, 75, "Activating schema drift detector")
        await self._log("info", "  Schema drift: 8 tables monitored · downstream impact analysis enabled", 9)
        await asyncio.sleep(0.2)

        total_rows = sum(self.table_stats.get(t, {}).get("rows", 0) for t in TABLE_ORDER)
        avg_score = int(sum(READINESS_SCORES[t]["gold"] for t in TABLE_ORDER) / len(TABLE_ORDER))

        await self._stage_progress(9, 100, "Monitoring live")
        await self._log("success", f"  Pipeline complete: {total_rows:,} rows · avg AI score: {avg_score} · status: GOLD", 9)
        await self._stage_complete(9, "Monitoring", time.time() - t0)

    # ── Run all stages ────────────────────────────────────────────────────────
    async def run(self):
        self.start_time = time.time()
        stages = [
            self.stage_discovery,
            self.stage_assessment,
            self.stage_architecture,
            self.stage_extraction,
            self.stage_transform,
            self.stage_data_quality,
            self.stage_governance,
            self.stage_ai_optimize,
            self.stage_monitoring,
        ]
        for fn in stages:
            await fn()

        total = round(time.time() - self.start_time, 1)
        total_rows = sum(self.table_stats.get(t, {}).get("rows", 0) for t in TABLE_ORDER)
        avg_score = int(sum(READINESS_SCORES[t]["gold"] for t in TABLE_ORDER) / len(TABLE_ORDER))

        await self.broadcast({
            "type": "pipeline_complete",
            "duration": total,
            "total_rows": total_rows,
            "tables_processed": len(TABLE_ORDER),
            "avg_ai_score": avg_score,
        })
        self.state["status"] = "complete"
