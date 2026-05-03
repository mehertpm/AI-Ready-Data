"""
AI Ready Data — POC Backend
FastAPI server with WebSocket for live pipeline progress.
Run: uvicorn main:app --reload --port 8000
"""
import asyncio
import os
import json
import time
from pathlib import Path
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

from oracle_sim import create_database, get_table_stats, READINESS_SCORES, TABLE_DQ_ISSUES, SCORE_FACTORS
from pipeline import PipelineEngine, TABLE_ORDER, STAGE_NAMES

# ── Bootstrap ─────────────────────────────────────────────────────────────────
print("Initializing Oracle simulation database...")
create_database()
print("Database ready.")

app = FastAPI(title="AI Ready Data POC", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])

FRONTEND_DIR = Path(__file__).parent.parent / "frontend"

# ── In-memory state ───────────────────────────────────────────────────────────
_pipeline_running = False
_pipeline_state = {
    "status": "idle",
    "current_stage": 0,
    "stages_complete": [],
    "tables": {},
    "dq_results": {},
    "lineage": {},
    "logs": [],
    "duration": 0,
}
_clients: list[WebSocket] = []


def _initial_table_state():
    stats = get_table_stats()
    return {
        tbl: {
            "layer": None,
            "score": None,
            "rows_processed": stats.get(tbl, {}).get("rows", 0),
            "rows_quarantined": 0,
            "domain": stats.get(tbl, {}).get("domain", ""),
            "pii": stats.get(tbl, {}).get("pii", []),
            "cols": stats.get(tbl, {}).get("cols", 0),
            "display": stats.get(tbl, {}).get("display", tbl),
        }
        for tbl in TABLE_ORDER
    }


def _build_full_state():
    return {
        "type": "full_state",
        "status": _pipeline_state["status"],
        "current_stage": _pipeline_state["current_stage"],
        "stages_complete": _pipeline_state["stages_complete"],
        "tables": _pipeline_state["tables"],
        "dq_results": _pipeline_state.get("dq_results", {}),
        "lineage": _pipeline_state.get("lineage", {}),
        "logs": _pipeline_state["logs"][-200:],  # last 200 log lines
        "stage_names": STAGE_NAMES,
        "table_order": TABLE_ORDER,
    }


async def _broadcast(msg: dict):
    """Send message to all connected WebSocket clients."""
    # Persist some state
    if msg.get("type") == "log":
        _pipeline_state["logs"].append(msg)
    elif msg.get("type") == "stage_start":
        _pipeline_state["current_stage"] = msg["stage"]
    elif msg.get("type") == "stage_complete":
        _pipeline_state["stages_complete"].append(msg["stage"])
    elif msg.get("type") == "table_update":
        tbl = msg["table"]
        existing = _pipeline_state["tables"].get(tbl, {})
        _pipeline_state["tables"][tbl] = {**existing, **{
            k: v for k, v in msg.items() if k != "type"
        }}
    elif msg.get("type") == "dq_result":
        _pipeline_state["dq_results"][msg["table"]] = msg.get("rules", {})
    elif msg.get("type") == "lineage_update":
        _pipeline_state["lineage"] = msg.get("lineage", {})
    elif msg.get("type") == "pipeline_complete":
        _pipeline_state["status"] = "complete"
        _pipeline_state["duration"] = msg.get("duration", 0)

    dead = []
    for client in _clients:
        try:
            await client.send_json(msg)
        except Exception:
            dead.append(client)
    for d in dead:
        _clients.remove(d)


# ── Routes ────────────────────────────────────────────────────────────────────
@app.get("/")
async def serve_index():
    index = FRONTEND_DIR / "index.html"
    if index.exists():
        return FileResponse(index)
    return HTMLResponse("<h1>Frontend not found — check frontend/index.html</h1>", status_code=404)


@app.get("/api/tables")
async def api_tables():
    stats = get_table_stats()
    result = {}
    for tbl in TABLE_ORDER:
        result[tbl] = {
            **stats.get(tbl, {}),
            "readiness_scores": READINESS_SCORES.get(tbl, {}),
            "dq_issues": TABLE_DQ_ISSUES.get(tbl, {}),
            "score_factors": SCORE_FACTORS.get(tbl, {}),
        }
    return result


@app.get("/api/state")
async def api_state():
    return _build_full_state()


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    global _pipeline_running
    await websocket.accept()
    _clients.append(websocket)

    # Send current state on connect
    await websocket.send_json(_build_full_state())

    try:
        while True:
            raw = await websocket.receive_text()
            msg = json.loads(raw)

            if msg.get("type") == "run_pipeline":
                if _pipeline_running:
                    await websocket.send_json({"type": "error", "message": "Pipeline already running"})
                    continue
                _pipeline_running = True
                # Reset state
                _pipeline_state.update({
                    "status": "running",
                    "current_stage": 0,
                    "stages_complete": [],
                    "tables": _initial_table_state(),
                    "dq_results": {},
                    "lineage": {},
                    "logs": [],
                    "duration": 0,
                })
                await _broadcast({"type": "pipeline_reset"})

                async def run_pipeline():
                    global _pipeline_running
                    try:
                        engine = PipelineEngine(_broadcast)
                        await engine.run()
                    finally:
                        _pipeline_running = False

                asyncio.create_task(run_pipeline())

            elif msg.get("type") == "reset":
                _pipeline_running = False
                _pipeline_state.update({
                    "status": "idle",
                    "current_stage": 0,
                    "stages_complete": [],
                    "tables": {},
                    "dq_results": {},
                    "lineage": {},
                    "logs": [],
                    "duration": 0,
                })
                await _broadcast({"type": "pipeline_reset"})

    except WebSocketDisconnect:
        if websocket in _clients:
            _clients.remove(websocket)


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False,
                log_level="info")
