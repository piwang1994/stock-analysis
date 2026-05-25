#!/usr/bin/env bash
# 启动个股分析网页端
set -euo pipefail
cd "$(dirname "$0")"
pip install -q -r requirements.txt
exec uvicorn web.app:app --host 0.0.0.0 --port 8000 --reload
