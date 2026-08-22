#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
./build.sh
java -ea -cp "out:lib/sqlite-jdbc-3.49.1.0.jar" scheduler.JobSchedulerTest
