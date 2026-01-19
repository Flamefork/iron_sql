test +args="":
    uv run pytest -vv --color=yes --showlocals --no-header '{{ args }}'

format:
    uv run ruff format .
    uv run ruff check . --fix || true

lint:
    uv run ruff format --check .
    uv run ruff check .
    uv run basedpyright

coverage:
    rm -rf .coverage/*
    uv run pytest --cov --cov-report=html
    open .coverage/htmlcov/index.html

install-deps:
    uv sync

update-deps: && install-deps
    uv lock --upgrade

release version:
    uv version {{ version }}
    git add --all
    git commit --message "Release v{{ version }}"
    git push
    git tag --annotate v{{ version }} --message v{{ version }}
    git push --tags
