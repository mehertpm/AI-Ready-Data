#!/bin/bash
set -e
echo "========================================"
echo "  AI Ready Data — Healthcare POC"
echo "  Epic EHR · HIPAA · FHIR R4"
echo "========================================"
cd "$(dirname "$0")/backend"
echo "→ Starting server on http://localhost:8001"
python3 main.py
