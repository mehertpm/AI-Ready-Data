#!/bin/bash
set -e
echo "========================================"
echo "  AI Ready Data — Pipeline POC"
echo "========================================"

cd "$(dirname "$0")/backend"

echo "→ Installing dependencies..."
pip install -r requirements.txt -q

echo "→ Starting server on http://localhost:8000"
echo "   Open your browser to http://localhost:8000"
echo "   Press Ctrl+C to stop"
echo ""
python main.py
