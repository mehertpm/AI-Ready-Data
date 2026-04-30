"""
9-Stage Healthcare Data Pipeline Engine.
Healthcare-specific logic: HIPAA Safe Harbor de-identification,
ICD-10/CPT/LOINC/NDC/NPI code validation, clinical DQ rules,
FHIR R4 alignment, and readmission risk feature engineering.
"""
import asyncio, time, random
from health_oracle_sim import (
    get_table_stats, TABLE_DQ_ISSUES, READINESS_SCORES, HIPAA_SCORES,
    SCORE_FACTORS, PHI_DETECTED, TABLE_ORDER, PHI_IDENTIFIERS_18
)

random.seed(42)

STAGE_NAMES = [
    "Discovery","Assessment","Architecture","Extraction",
    "Transform","Data Quality","Governance","AI Optimize","Monitoring"
]

# 7 Healthcare-specific DQ rules
HEALTH_DQ_RULES = [
    ("phi_completeness",   "PHI Completeness",      "Required HIPAA identifiers present & formatted"),
    ("icd10_validity",     "ICD-10-CM Validity",    "Codes match ICD-10-CM 2024 format & code set"),
    ("lab_range",          "Lab Reference Range",   "Results within 3× clinical reference bounds"),
    ("medication_safety",  "Medication Safety",     "Dosage within therapeutic range per drug class"),
    ("claim_integrity",    "Claim Integrity",       "Paid ≤ Billed, valid NPI, coherent service dates"),
    ("date_coherence",     "Date Coherence",        "Admit < Discharge, DOB < Service, Result ≥ Collection"),
    ("patient_dedup_mpi",  "Patient Dedup (MPI)",   "Probabilistic master patient index matching"),
]


class HealthPipelineEngine:
    def __init__(self, broadcast_fn):
        self.broadcast = broadcast_fn
        self.start_time = None
        self.table_stats = {}
        self.state = {
            "status": "running", "current_stage": 0,
            "stages_complete": [], "tables": {},
            "dq_results": {}, "lineage": {},
            "hipaa_scores": {}, "phi_summary": {},
        }

    async def _log(self, level, msg, stage=None):
        await self.broadcast({"type":"log","level":level,"message":msg,
                               "stage":stage,"ts":time.strftime("%H:%M:%S")})

    async def _stage_start(self, n, name):
        self.state["current_stage"] = n
        await self.broadcast({"type":"stage_start","stage":n,"name":name})
        await self._log("info", f"▶  Stage {n}: {name} — started", n)

    async def _stage_progress(self, n, pct, msg):
        await self.broadcast({"type":"stage_progress","stage":n,"progress":pct,"message":msg})

    async def _stage_complete(self, n, name, dur):
        self.state["stages_complete"].append(n)
        await self.broadcast({"type":"stage_complete","stage":n,"name":name,"duration":round(dur,2)})
        await self._log("success", f"✔  Stage {n}: {name} — complete ({dur:.1f}s)", n)

    async def _table_update(self, tbl, layer, score, hipaa_score=None,
                             rows_processed=None, rows_quarantined=0, extra=None):
        rows = rows_processed or self.table_stats.get(tbl,{}).get("rows",0)
        self.state["tables"][tbl] = {
            "layer": layer, "score": score, "hipaa_score": hipaa_score,
            "rows_processed": rows, "rows_quarantined": rows_quarantined,
            **(extra or {}),
        }
        await self.broadcast({
            "type":"table_update","table":tbl,"layer":layer,"score":score,
            "hipaa_score": hipaa_score,
            "rows_processed":rows,"rows_quarantined":rows_quarantined,
            **(extra or {}),
        })

    # ── Stage 1: Discovery ────────────────────────────────────────────────────
    async def stage_discovery(self):
        t0 = time.time()
        await self._stage_start(1, "Discovery")
        await self._log("info", "  Connecting to Epic EHR Oracle backend at EPIC-UAT.hospital.org:1521/EPICPRD", 1)
        await asyncio.sleep(0.4)
        await self._stage_progress(1, 8, "Authenticated as EHR_PIPELINE_SVC — scanning clinical schema")
        await asyncio.sleep(0.3)

        self.table_stats = get_table_stats()
        total = len(TABLE_ORDER)
        phi_total = 0

        for idx, tbl in enumerate(TABLE_ORDER):
            meta = self.table_stats.get(tbl, {})
            pct = 12 + int((idx / total) * 55)
            await self._stage_progress(1, pct, f"Profiling {tbl} ({meta.get('fhir','?')} resource)...")
            phi_cols = meta.get("phi", [])
            phi_total += len(phi_cols)
            await self._log("info",
                f"  → {tbl} ({meta.get('fhir','?')}): {meta.get('rows',0):,} rows · "
                f"{meta.get('cols',0)} cols · PHI: {phi_cols} · domain: {meta.get('domain','?')}", 1)
            await asyncio.sleep(0.20)
            await self._table_update(tbl, "source", READINESS_SCORES[tbl]["source"])

        await self._stage_progress(1, 72, "Scanning for 18 HIPAA PHI identifiers...")
        await asyncio.sleep(0.3)

        # PHI summary across tables
        phi_found = {ident: any(PHI_DETECTED.get(t,{}).get(ident,False) for t in TABLE_ORDER)
                     for ident in PHI_IDENTIFIERS_18}
        found_count = sum(phi_found.values())
        self.state["phi_summary"] = phi_found
        await self.broadcast({"type":"phi_summary","phi": phi_found,"found_count":found_count})
        await self._log("warn",
            f"  ⚠️  PHI DETECTED: {found_count}/18 HIPAA identifiers found across {len(TABLE_ORDER)} tables — "
            f"Safe Harbor de-identification required before analytics", 1)
        await self._stage_progress(1, 90, "Building FHIR resource dependency graph...")
        await asyncio.sleep(0.25)
        await self._log("info",
            "  FHIR graph: Encounter→Patient, Condition→Encounter, MedicationRequest→Patient, "
            "Observation→Encounter, Claim→Encounter, Procedure→Encounter", 1)
        await self._stage_progress(1, 100, "Discovery complete")
        await self._stage_complete(1, "Discovery", time.time() - t0)
        await asyncio.sleep(0.3)

    # ── Stage 2: Assessment ───────────────────────────────────────────────────
    async def stage_assessment(self):
        t0 = time.time()
        await self._stage_start(2, "Assessment")
        await self._log("info", "  Computing AI Readiness Score AND HIPAA Compliance Score per table", 2)
        await asyncio.sleep(0.2)

        for idx, tbl in enumerate(TABLE_ORDER):
            pct = 5 + int((idx / len(TABLE_ORDER)) * 88)
            await self._stage_progress(2, pct, f"Scoring {tbl}...")
            f = SCORE_FACTORS[tbl]
            ai_score = int(
                f["phi_completeness"] * 0.25 +
                f["clinical_code_validity"] * 0.25 +
                f["temporal_coherence"] * 0.20 +
                f["referential_integrity"] * 0.15 +
                f["deid_readiness"] * 0.15
            )
            hipaa = HIPAA_SCORES[tbl]["bronze"]
            issues = TABLE_DQ_ISSUES[tbl]
            await self._log("info",
                f"  {tbl}: AI={ai_score} | HIPAA={hipaa} | "
                f"PHI_completeness={f['phi_completeness']} Code_validity={f['clinical_code_validity']} "
                f"Temporal={f['temporal_coherence']} DeID={f['deid_readiness']} | "
                f"ICD_invalid={issues['invalid_code_pct']}% Date_err={issues['date_error_pct']}%", 2)
            await self._table_update(tbl, "source", ai_score, hipaa_score=hipaa,
                                      extra={"factors": f})
            await asyncio.sleep(0.18)

        await self._stage_progress(2, 100, "Assessment complete")
        await self._stage_complete(2, "Assessment", time.time() - t0)
        await asyncio.sleep(0.3)

    # ── Stage 3: Architecture ─────────────────────────────────────────────────
    async def stage_architecture(self):
        t0 = time.time()
        await self._stage_start(3, "Architecture")
        await self._log("info", "  Auto-generating FHIR R4-aligned medallion DDL", 3)
        await asyncio.sleep(0.2)

        layers = [
            ("Bronze",   "Raw PHI landing zone — restricted access, full audit log"),
            ("Silver",   "HIPAA Safe Harbor de-identified — research & analytics zone"),
            ("Gold",     "FHIR R4 validated, DQ-certified — clinical intelligence zone"),
            ("Platinum", "ML feature store — readmission risk, LOS prediction, NLP embeddings"),
        ]
        for idx, (layer, desc) in enumerate(layers):
            pct = 10 + idx * 20
            await self._stage_progress(3, pct, f"Generating {layer} DDL...")
            await self._log("info", f"  {layer}: {desc}", 3)
            await asyncio.sleep(0.22)

        await self._stage_progress(3, 90, "Generating FHIR-to-Delta mapping notebooks...")
        await self._log("info",
            "  FHIR R4 resource mappings: Patient→PATIENTS, Encounter→ENCOUNTERS, "
            "Condition→DIAGNOSES, MedicationRequest→MEDICATIONS, Observation→LAB_RESULTS, "
            "Procedure→PROCEDURES, Claim→CLAIMS, Practitioner→PROVIDERS", 3)
        await asyncio.sleep(0.2)
        await self._stage_progress(3, 100, "Architecture complete — HIPAA access zones defined")
        await self._stage_complete(3, "Architecture", time.time() - t0)
        await asyncio.sleep(0.3)

    # ── Stage 4: Extraction ───────────────────────────────────────────────────
    async def stage_extraction(self):
        t0 = time.time()
        await self._stage_start(4, "Extraction")
        await self._log("info",
            "  8-way parallel extraction — PHI audit log enabled — Bronze zone (restricted)", 4)
        await asyncio.sleep(0.2)

        tasks = []
        sem = asyncio.Semaphore(8)

        async def load_table(tbl, idx):
            async with sem:
                rows = self.table_stats.get(tbl, {}).get("rows", 0)
                chunks = max(1, rows // 8000)
                dead = int(rows * TABLE_DQ_ISSUES[tbl]["phi_missing_pct"] / 100 * 0.1)
                for chunk in range(chunks):
                    pct = 5 + int(((idx * chunks + chunk) / (len(TABLE_ORDER) * chunks)) * 88)
                    done = min((chunk+1)*8000, rows)
                    await self._stage_progress(4, pct, f"[W-{idx+1}] {tbl}: {done:,}/{rows:,} rows")
                    await self._log("info",
                        f"  [W-{idx+1}] {tbl}: {done:,}/{rows:,} → Bronze (PHI audit logged)", 4)
                    await asyncio.sleep(0.12 + random.uniform(0, 0.08))
                score = READINESS_SCORES[tbl]["bronze"]
                hipaa = HIPAA_SCORES[tbl]["bronze"]
                await self._table_update(tbl, "bronze", score, hipaa_score=hipaa,
                                          rows_processed=rows, rows_quarantined=dead)
                await self._log("warn" if hipaa < 50 else "info",
                    f"  ✔ {tbl} → Bronze: {rows:,} rows · HIPAA={hipaa} "
                    f"{'⚠️ PHI EXPOSED' if hipaa < 50 else '(PHI in restricted zone)'}", 4)

        for idx, tbl in enumerate(TABLE_ORDER):
            tasks.append(load_table(tbl, idx))
        await asyncio.gather(*tasks)
        await self._stage_progress(4, 100, "All 8 clinical tables loaded to Bronze")
        await self._stage_complete(4, "Extraction", time.time() - t0)
        await asyncio.sleep(0.3)

    # ── Stage 5: Transform ────────────────────────────────────────────────────
    async def stage_transform(self):
        t0 = time.time()
        await self._stage_start(5, "Transform")
        await self._log("info",
            "  Running HIPAA Safe Harbor de-identification + ICD/LOINC/NDC validation + SCD2", 5)
        await asyncio.sleep(0.2)

        safe_harbor_steps = [
            "Removing/masking 18 PHI identifier categories",
            "Date shifting (rare conditions: year-only retained)",
            "Geographic generalization (5-digit ZIP → 3-digit)",
            "Age transformation (ages >89 grouped)",
        ]
        for step in safe_harbor_steps:
            await self._log("info", f"  Safe Harbor: {step}", 5)
            await asyncio.sleep(0.12)

        for idx, tbl in enumerate(TABLE_ORDER):
            pct = 20 + int((idx / len(TABLE_ORDER)) * 72)
            rows = self.table_stats.get(tbl, {}).get("rows", 0)
            issues = TABLE_DQ_ISSUES[tbl]
            dupes = int(rows * issues["duplicate_pct"] / 100)
            code_fixed = int(rows * issues["invalid_code_pct"] / 100)
            date_fixed = int(rows * issues["date_error_pct"] / 100)

            await self._stage_progress(5, pct, f"Transforming {tbl} → Silver")
            await self._log("info",
                f"  {tbl}: de-identified ({len(self.table_stats.get(tbl,{}).get('phi',[]))} PHI cols masked) · "
                f"ICD/CPT/LOINC corrected ({code_fixed:,}) · date-coherence fixed ({date_fixed:,}) · "
                f"SCD2 dedupe ({dupes:,} dups removed)", 5)
            await asyncio.sleep(0.28)

            silver_rows = rows - dupes
            score = READINESS_SCORES[tbl]["silver"]
            hipaa = HIPAA_SCORES[tbl]["silver"]
            await self._table_update(tbl, "silver", score, hipaa_score=hipaa,
                                      rows_processed=silver_rows)
            await self._log("success",
                f"  ✔ {tbl} → Silver: {silver_rows:,} rows · HIPAA={hipaa} 🔒 PHI de-identified", 5)

        await self._stage_progress(5, 100, "All tables at Silver — PHI de-identified")
        await self._stage_complete(5, "Transform", time.time() - t0)
        await asyncio.sleep(0.3)

    # ── Stage 6: Data Quality ─────────────────────────────────────────────────
    async def stage_data_quality(self):
        t0 = time.time()
        await self._stage_start(6, "Data Quality")
        await self._log("info",
            "  Applying 7 healthcare-specific DQ rules — clinical code validation, safety checks", 6)
        await asyncio.sleep(0.2)

        self.state["dq_results"] = {}

        for idx, tbl in enumerate(TABLE_ORDER):
            pct = 5 + int((idx / len(TABLE_ORDER)) * 86)
            await self._stage_progress(6, pct, f"Clinical DQ: {tbl}")
            rows = self.table_stats.get(tbl, {}).get("rows", 0)
            issues = TABLE_DQ_ISSUES[tbl]
            tbl_results = {}
            quarantined = 0

            rule_issue_map = {
                "phi_completeness":  issues["phi_missing_pct"],
                "icd10_validity":    issues["invalid_code_pct"],
                "lab_range":         issues["range_error_pct"],
                "medication_safety": issues["range_error_pct"],
                "claim_integrity":   issues["claim_error_pct"],
                "date_coherence":    issues["date_error_pct"],
                "patient_dedup_mpi": issues["duplicate_pct"],
            }

            for rule_id, rule_name, rule_desc in HEALTH_DQ_RULES:
                issue_pct = rule_issue_map.get(rule_id, 0)
                failed = int(rows * issue_pct / 100)
                passed = rows - failed
                tbl_results[rule_id] = {
                    "name": rule_name, "desc": rule_desc,
                    "passed": passed, "failed": failed,
                    "pass_rate": round(100 * passed / max(rows, 1), 1),
                }
                quarantined += failed
                if failed > 0:
                    lvl = "warn" if issue_pct < 10 else "error"
                    await self._log(lvl,
                        f"  [{tbl}] {rule_name}: {failed:,} records failed → quarantined", 6)
                else:
                    await self._log("info", f"  [{tbl}] {rule_name}: ✔ PASS", 6)

            self.state["dq_results"][tbl] = tbl_results
            await self.broadcast({"type":"dq_result","table":tbl,"rules":tbl_results,
                                   "quarantined":quarantined})
            score = READINESS_SCORES[tbl]["gold"]
            hipaa = HIPAA_SCORES[tbl]["gold"]
            gold_rows = max(0, rows - quarantined)
            await self._table_update(tbl, "gold", score, hipaa_score=hipaa,
                                      rows_processed=gold_rows, rows_quarantined=quarantined)
            await self._log("success",
                f"  ✔ {tbl} → Gold: {gold_rows:,} rows · HIPAA={hipaa} · AI={score} ✅ HIPAA COMPLIANT", 6)
            await asyncio.sleep(0.26)

        await self._stage_progress(6, 100, "All tables certified Gold — HIPAA compliant")
        await self._stage_complete(6, "Data Quality", time.time() - t0)
        await asyncio.sleep(0.3)

    # ── Stage 7: Governance ───────────────────────────────────────────────────
    async def stage_governance(self):
        t0 = time.time()
        await self._stage_start(7, "Governance")
        await self._log("info", "  HIPAA audit trail · Purview catalog · Consent management · BAA compliance", 7)
        await asyncio.sleep(0.3)

        governance_steps = [
            ("HIPAA Audit Trail",    "All PHI access events logged with user, timestamp, purpose"),
            ("BAA Compliance",       "Business Associate Agreement verified for all downstream consumers"),
            ("Consent Registry",     "Patient consent flags propagated to Silver/Gold/Platinum layers"),
            ("Minimum Necessary",    "Column-level access controls: only required fields exposed per role"),
            ("Purview Registration", "FHIR resources registered in Microsoft Purview with sensitivity labels"),
            ("Lineage Capture",      "Source Oracle → Bronze → Silver → Gold → Platinum full lineage tracked"),
        ]
        for idx, (step, detail) in enumerate(governance_steps):
            pct = 8 + idx * 14
            await self._stage_progress(7, pct, step)
            await self._log("info", f"  {step}: {detail}", 7)
            await asyncio.sleep(0.22)

        lineage = {}
        for tbl in TABLE_ORDER:
            meta = self.table_stats.get(tbl, {})
            lineage[tbl] = {
                "nodes": ["Epic Oracle Source", f"Bronze/{tbl}", f"Silver/{tbl} (de-id)", f"Gold/{tbl} (FHIR)", f"Platinum/{tbl} (ML)"],
                "fhir_resource": meta.get("fhir","?"),
                "phi_columns": meta.get("phi",[]),
            }
            self.state["lineage"][tbl] = lineage[tbl]

        await self.broadcast({"type":"lineage_update","lineage":self.state["lineage"]})
        await self._stage_progress(7, 100, "Governance complete — audit-ready")
        await self._stage_complete(7, "Governance", time.time() - t0)
        await asyncio.sleep(0.3)

    # ── Stage 8: AI Optimize ──────────────────────────────────────────────────
    async def stage_ai_optimize(self):
        t0 = time.time()
        await self._stage_start(8, "AI Optimize")
        await self._log("info",
            "  Clinical ML features: 30-day readmission risk · LOS prediction · ICD NLP embeddings · RAG chunks", 8)
        await asyncio.sleep(0.2)

        features_map = {
            "PATIENTS":   "age_band · comorbidity_index · chronic_condition_count · insurance_type_encoded",
            "ENCOUNTERS": "los_days · admission_source_encoded · drg_weight · readmission_flag_30d",
            "DIAGNOSES":  "icd10_embedding_128d · chronic_vs_acute_flag · severity_score · comorbidity_elixhauser",
            "MEDICATIONS":"polypharmacy_flag · high_risk_drug_flag · adherence_score · drug_class_vector",
            "LAB_RESULTS":"abnormal_flag_count · critical_value_alert · trend_7d · z_score_vs_reference",
            "PROCEDURES": "procedure_complexity_score · cpt_category_encoded · prior_auth_required_flag",
            "CLAIMS":     "denial_risk_score · cost_per_encounter · payer_mix_flag · revenue_cycle_vector",
            "PROVIDERS":  "case_mix_index · readmission_rate · quality_score · specialty_embedding",
        }
        for idx, tbl in enumerate(TABLE_ORDER):
            pct = 5 + int((idx / len(TABLE_ORDER)) * 88)
            rows = self.table_stats.get(tbl, {}).get("rows", 0)
            await self._stage_progress(8, pct, f"Clinical features: {tbl}")
            await self._log("info", f"  {tbl}: {features_map.get(tbl,'feature_vector_128d')}", 8)
            await asyncio.sleep(0.22)
            score = READINESS_SCORES[tbl]["platinum"]
            hipaa = HIPAA_SCORES[tbl]["platinum"]
            await self._table_update(tbl, "platinum", score, hipaa_score=hipaa, rows_processed=rows)

        await self._log("info",
            "  RAG text chunking: DIAGNOSES.icd10_description, PROCEDURES.procedure_name → "
            f"LLM-ready chunks (512 tok) for clinical co-pilot grounding", 8)
        await self._stage_progress(8, 100, "Platinum layer complete — AI-ready")
        await self._stage_complete(8, "AI Optimize", time.time() - t0)
        await asyncio.sleep(0.3)

    # ── Stage 9: Monitoring ───────────────────────────────────────────────────
    async def stage_monitoring(self):
        t0 = time.time()
        await self._stage_start(9, "Monitoring")
        await self._log("info", "  Clinical quality monitoring · HIPAA access alerts · Patient safety dashboards", 9)
        await asyncio.sleep(0.3)

        monitors = [
            "PHI access anomaly detection (>5 patient records/min triggers alert)",
            "Critical lab value alerts (panic values → clinical notification pipeline)",
            "Medication safety watchdog (dosage outliers → pharmacist review queue)",
            "Claim denial rate monitoring (>15% denial rate triggers revenue cycle alert)",
            "ICD-10 code drift detection (new codes from annual CM update)",
            "Patient readmission risk threshold alerts (score >0.7 → care management)",
        ]
        for i, mon in enumerate(monitors):
            pct = 10 + i * 14
            await self._stage_progress(9, pct, "Activating monitor...")
            await self._log("info", f"  Monitor {i+1}: {mon}", 9)
            await asyncio.sleep(0.18)

        total_rows = sum(self.table_stats.get(t,{}).get("rows",0) for t in TABLE_ORDER)
        avg_ai = int(sum(READINESS_SCORES[t]["gold"] for t in TABLE_ORDER) / len(TABLE_ORDER))
        avg_hipaa = int(sum(HIPAA_SCORES[t]["gold"] for t in TABLE_ORDER) / len(TABLE_ORDER))
        await self._stage_progress(9, 100, "Clinical monitoring active")
        await self._log("success",
            f"  Pipeline complete: {total_rows:,} clinical records · "
            f"AI score: {avg_ai} · HIPAA score: {avg_hipaa} · Status: GOLD ✅ COMPLIANT", 9)
        await self._stage_complete(9, "Monitoring", time.time() - t0)

    # ── Run all ───────────────────────────────────────────────────────────────
    async def run(self):
        self.start_time = time.time()
        for fn in [self.stage_discovery, self.stage_assessment, self.stage_architecture,
                   self.stage_extraction, self.stage_transform, self.stage_data_quality,
                   self.stage_governance, self.stage_ai_optimize, self.stage_monitoring]:
            await fn()

        total = round(time.time() - self.start_time, 1)
        total_rows = sum(self.table_stats.get(t,{}).get("rows",0) for t in TABLE_ORDER)
        avg_ai    = int(sum(READINESS_SCORES[t]["gold"] for t in TABLE_ORDER) / len(TABLE_ORDER))
        avg_hipaa = int(sum(HIPAA_SCORES[t]["gold"] for t in TABLE_ORDER) / len(TABLE_ORDER))
        await self.broadcast({
            "type":"pipeline_complete","duration":total,
            "total_rows":total_rows,"tables_processed":len(TABLE_ORDER),
            "avg_ai_score":avg_ai,"avg_hipaa_score":avg_hipaa,
        })
        self.state["status"] = "complete"
