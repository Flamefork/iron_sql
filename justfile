format:
    uv run ruff format .
    uv run ruff check . --fix || true

lint:
    uv run ruff format --check .
    uv run ruff check .
    uv run basedpyright

test +args="":
    uv run pytest -vv --color=yes --showlocals '{{ args }}'

coverage:
    rm -rf .coverage/*
    uv run pytest --cov --cov-report=html
    open .coverage/htmlcov/index.html

generate-example:
    uv run python example/generate.py

release version:
    uv version {{ version }}
    git add --all
    git commit --message "Release v{{ version }}"
    git push
    git tag --annotate v{{ version }} --message v{{ version }}
    git push --tags
