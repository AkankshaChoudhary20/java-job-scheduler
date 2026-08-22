#!/usr/bin/env bash
# Vendors the single sqlite-jdbc jar this project needs -- deliberately not
# committed to git (it's a 14MB binary), and deliberately not Maven/Gradle
# (this project stays a plain-JDK, javac/java project).
set -euo pipefail
cd "$(dirname "$0")/.."

VERSION="3.49.1.0"
mkdir -p lib
if [ ! -f "lib/sqlite-jdbc-${VERSION}.jar" ]; then
    curl -sL "https://repo1.maven.org/maven2/org/xerial/sqlite-jdbc/${VERSION}/sqlite-jdbc-${VERSION}.jar" \
        -o "lib/sqlite-jdbc-${VERSION}.jar"
fi
echo "lib/sqlite-jdbc-${VERSION}.jar ready"
