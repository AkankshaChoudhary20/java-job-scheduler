#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
./build.sh
java -cp "out:lib/sqlite-jdbc-3.49.1.0.jar" scheduler.Main "${1:-scheduler-demo.db}" "${2:-4}"
