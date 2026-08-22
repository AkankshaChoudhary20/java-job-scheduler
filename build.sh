#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
./scripts/fetch-deps.sh
mkdir -p out
javac -cp "lib/sqlite-jdbc-3.49.1.0.jar" -d out $(find src/main src/test -name "*.java")
echo "compiled to out/"
