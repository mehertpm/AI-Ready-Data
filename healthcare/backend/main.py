"""
AI Ready Data — Healthcare POC Backend
FastAPI on port 8001. Run: python main.py
"""
import asyncio, os, json, time
from pathlib import Path
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware

from health_oracle_sim import (
    create_database, get_table_stats,
    READINESS_SCORES, HIPAA_SCORES, TABLE_DQ_ISSUES, SCORE_FACTORS, PHI_DETECTED,
    TABLE_ORDER, PHI_IDENTIFIERS_18
)
from health_pipeline import HealthPipelineEngine, STAGE_NAMES

print("Initializing Healthcare Oracle simulation database...")
create_database()
print("Healthcare database ready.")

app = FastAPI(title="AI Ready Data — Healthcare POC", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])

FRONTEND_DIR = Path(__file__).parent.parent / "frontend"

_pipeline_running = False
_state = {
    "status": "idle", "current_stage": 0, "stages_complete": [],
    "tables": {}, "dq_results": {}, "lineage": {},
    "phi_summary": {}, "logs": [], "duration": 0,
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
        "phi_summary": _state.get("phi_summary", {}),
        "logs": _state["logs"][-200:],
        "stage_names": STAGE_NAMES,
        "table_order": TABLE_ORDER,
        "phi_identifiers": PHI_IDENTIFIERS_18,
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
    elif msg.get("type") == "phi_summary":
        _state["phi_summary"] = msg.get("phi", {})
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
                  "hipaa_scores": HIPAA_SCORES.get(tbl, {}),
                  "dq_issues": TABLE_DQ_ISSUES.get(tbl, {}),
                  "phi_detected": PHI_DETECTED.get(tbl, {})}
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
                    await websocket.send_json({"type":"error","message":"Already running"})
                    continue
                _pipeline_running = True
                stats = get_table_stats()
                _state.update({
                    "status":"running","current_stage":0,"stages_complete":[],
                    "tables":{tbl: {
                        "layer":None,"score":None,"hipaa_score":None,
                        "rows_processed":stats.get(tbl,{}).get("rows",0),
                        "rows_quarantined":0,
                        "domain":stats.get(tbl,{}).get("domain",""),
                        "phi":stats.get(tbl,{}).get("phi",[]),
                        "fhir":stats.get(tbl,{}).get("fhir",""),
                        "display":stats.get(tbl,{}).get("display",tbl),
                    } for tbl in TABLE_ORDER},
                    "dq_results":{},"lineage":{},"phi_summary":{},"logs":[],"duration":0,
                })
                await _broadcast({"type":"pipeline_reset"})

                async def run_it():
                    global _pipeline_running
                    try:
                        engine = HealthPipelineEngine(_broadcast)
                        await engine.run()
                    finally:
                        _pipeline_running = False
                asyncio.create_task(run_it())

            elif msg.get("type") == "reset":
                _pipeline_running = False
                _state.update({
                    "status":"idle","current_stage":0,"stages_complete":[],
                    "tables":{},"dq_results":{},"lineage":{},"phi_summary":{},"logs":[],"duration":0,
                })
                await _broadcast({"type":"pipeline_reset"})

    except WebSocketDisconnect:
        if websocket in _clients:
            _clients.remove(websocket)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8001, reload=False, log_level="info")
