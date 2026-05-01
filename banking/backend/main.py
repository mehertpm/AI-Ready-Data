"""
AI Ready Data — Banking POC Backend
FastAPI on port 8002. Run: python main.py
"""
import asyncio, os, json, time
from pathlib import Path
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware

from bank_oracle_sim import (
    create_database, get_table_stats,
    READINESS_SCORES, COMPLIANCE_SCORES, TABLE_DQ_ISSUES,
    SCORE_FACTORS, PHI_DETECTED, IMPROVEMENT_PATHS,
    PCI_IDENTIFIERS, AML_KYC_FIELDS, TABLE_ORDER,
)
from bank_pipeline import BankPipelineEngine, STAGE_NAMES

print("Initializing Banking simulation database...")
create_database()
print("Banking database ready.")

app = FastAPI(title="AI Ready Data — Banking POC", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])

FRONTEND_DIR = Path(__file__).parent.parent / "frontend"

_pipeline_running = False
_state = {
    "status": "idle", "current_stage": 0, "stages_complete": [],
    "tables": {}, "dq_results": {}, "lineage": {},
    "compliance_summary": {}, "logs": [], "duration": 0,
}
_clients: list[WebSocket] = []


def _full_state():
    return {
        "type": "full_state",
        "status": _state["status"],
        "current_stage": _state["current_stage"],
        "stages_complete": _state["stages_complete"],
        "tables": _state["tables"],
        "dq_results": _state.get("dq_results", {}),
        "lineage": _state.get("lineage", {}),
        "compliance_summary": _state.get("compliance_summary", {}),
        "logs": _state["logs"][-200:],
        "stage_names": STAGE_NAMES,
        "table_order": TABLE_ORDER,
        "pci_identifiers": PCI_IDENTIFIERS,
        "aml_kyc_fields": AML_KYC_FIELDS,
        "improvement_paths": IMPROVEMENT_PATHS,
    }


async def _broadcast(msg: dict):
    if msg.get("type") == "log":
        _state["logs"].append(msg)
    elif msg.get("type") == "stage_start":
        _state["current_stage"] = msg["stage"]
    elif msg.get("type") == "stage_complete":
        _state["stages_complete"].append(msg["stage"])
    elif msg.get("type") == "table_update":
        tbl = msg["table"]
        _state["tables"][tbl] = {**_state["tables"].get(tbl, {}),
                                   **{k: v for k, v in msg.items() if k != "type"}}
    elif msg.get("type") == "dq_result":
        _state["dq_results"][msg["table"]] = msg.get("rules", {})
    elif msg.get("type") == "lineage_update":
        _state["lineage"] = msg.get("lineage", {})
    elif msg.get("type") == "compliance_summary":
        _state["compliance_summary"] = {k: v for k, v in msg.items() if k != "type"}
    elif msg.get("type") == "pipeline_complete":
        _state["status"] = "complete"
        _state["duration"] = msg.get("duration", 0)

    dead = []
    for client in _clients:
        try:
            await client.send_json(msg)
        except Exception:
            dead.append(client)
    for d in dead:
        _clients.remove(d)


@app.get("/")
async def serve_index():
    idx = FRONTEND_DIR / "index.html"
    if idx.exists():
        return FileResponse(idx)
    return HTMLResponse("<h1>Frontend not found</h1>", 404)


@app.get("/api/tables")
async def api_tables():
    stats = get_table_stats()
    return {tbl: {**stats.get(tbl, {}),
                  "readiness_scores": READINESS_SCORES.get(tbl, {}),
                  "compliance_scores": COMPLIANCE_SCORES.get(tbl, {}),
                  "dq_issues": TABLE_DQ_ISSUES.get(tbl, {}),
                  "phi_detected": PHI_DETECTED.get(tbl, {}),
                  "improvement_paths": IMPROVEMENT_PATHS.get(tbl, {})}
            for tbl in TABLE_ORDER}


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    global _pipeline_running
    await websocket.accept()
    _clients.append(websocket)
    await websocket.send_json(_full_state())

    try:
        while True:
            raw = await websocket.receive_text()
            msg = json.loads(raw)

            if msg.get("type") == "run_pipeline":
                if _pipeline_running:
                    await websocket.send_json({"type": "error", "message": "Already running"})
                    continue
                _pipeline_running = True
                stats = get_table_stats()
                _state.update({
                    "status": "running", "current_stage": 0, "stages_complete": [],
                    "tables": {tbl: {
                        "layer": None, "score": None, "compliance_score": None,
                        "rows_processed": stats.get(tbl, {}).get("rows", 0),
                        "rows_quarantined": 0,
                        "domain": stats.get(tbl, {}).get("domain", ""),
                        "regulatory": stats.get(tbl, {}).get("regulatory", ""),
                        "display": stats.get(tbl, {}).get("display", tbl),
                    } for tbl in TABLE_ORDER},
                    "dq_results": {}, "lineage": {}, "compliance_summary": {},
                    "logs": [], "duration": 0,
                })
                await _broadcast({"type": "pipeline_reset"})

                async def run_it():
                    global _pipeline_running
                    try:
                        engine = BankPipelineEngine(_broadcast)
                        await engine.run()
                    finally:
                        _pipeline_running = False
                asyncio.create_task(run_it())

            elif msg.get("type") == "reset":
                _pipeline_running = False
                _state.update({
                    "status": "idle", "current_stage": 0, "stages_complete": [],
                    "tables": {}, "dq_results": {}, "lineage": {},
                    "compliance_summary": {}, "logs": [], "duration": 0,
                })
                await _broadcast({"type": "pipeline_reset"})

    except WebSocketDisconnect:
        if websocket in _clients:
            _clients.remove(websocket)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8002, reload=False, log_level="info")
