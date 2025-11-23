repo_root="$(git rev-parse --show-toplevel)"
export PYTHONPATH="$repo_root"

cd "$repo_root"

pytest "tests/unit" -v --ignore="logs"

