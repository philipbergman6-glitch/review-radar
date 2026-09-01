#!/usr/bin/env bash
# Wrapper that pins JAVA_HOME (Spark needs JDK 17) and runs inside the uv env.
set -euo pipefail
export JAVA_HOME="${JAVA_HOME:-/opt/homebrew/opt/openjdk@17}"
export PATH="$JAVA_HOME/bin:$PATH"
exec uv run "$@"
