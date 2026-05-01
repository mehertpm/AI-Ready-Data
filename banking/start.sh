#!/bin/bash
set -e
echo "========================================"
echo "  AI Ready Data — Banking POC"
echo "  BSA/AML · PCI-DSS · KYC/CDD · HMDA"
echo "========================================"
cd "$(dirname "$0")/backend"
echo "→ Starting server on http://localhost:8002"
python3 main.py
