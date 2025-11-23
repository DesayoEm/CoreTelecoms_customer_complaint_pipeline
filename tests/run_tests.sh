repo_root="$(git rev-parse --show-toplevel)"
export PYTHONPATH="$repo_root"

cd "$repo_root"

pytest "tests/unit/transformation/test_data_cleaning.py" -v --ignore="logs"
