format:
    uv run ruff format .
    uv run ruff check . --fix || true

lint:
    uv run ruff format --check .
    uv run ruff check .
    uv run basedpyright

test:
    uv run pytest -vv --color=yes --showlocals --durations=10 --cov=iron_sql

[positional-arguments]
pytest *args:
    uv run pytest -vv --color=yes --showlocals --durations=10 "$@"

coverage:
    rm -rf .coverage/*
    uv run pytest --cov=iron_sql --cov-report=html
    open .coverage/htmlcov/index.html

generate-example:
    uv run python -m example.generate

release version:
    uv version {{ version }}
    git add --all
    git commit --message "Release v{{ version }}"
    git push
    git tag --annotate v{{ version }} --message v{{ version }}
    git push --tags
