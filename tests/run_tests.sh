#!/bin/bash

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export PYTHONPATH="$PROJECT_ROOT"

python3 -m pytest "$PROJECT_ROOT/" -v --ignore="$PROJECT_ROOT/logs"