'''
#!/usr/bin/env bash
set -euo pipefail

case "${1:-}" in
  extract)
    echo "⏳  Extracting raw data…"
    python scripts/Extract.py
    ;;
  transform_meetings)
    echo "🔄  Transforming meetings…"
    python scripts/meetings_transform.py
    ;;
  transform_sessions)
    echo "🔄  Transforming sessions…"
    python scripts/sessions_transform.py
    ;;
  transform_drivers)
    echo "🔄  Transforming drivers…"
    python scripts/drivers_transform.py
    ;;
  transform_results)
    echo "🔄  Transforming session results…"
    python scripts/sessionresults_transform.py
    ;;
  transform_grid)
    echo "🔄  Transforming starting grid…"
    python scripts/startinggrid_transform.py
    ;;
  load)
    echo "🔄  Loading extracted data"
    python scripts/load.py
    ;; 
  all)
    echo "🚀  Running full ETL: extract + all transforms"
    python scripts/Extract.py
    python scripts/meetings_transform.py
    python scripts/sessions_transform.py
    python scripts/drivers_transform.py
    python scripts/sessionresults_transform.py
    python scripts/startinggrid_transform.py
    python scripts/load.py
    ;;
  *)
    echo "Usage: $0 {extract|transform_meetings|transform_sessions|transform_drivers|transform_results|transform_grid|load|all}"
    exit 1
    ;;
esac
'''