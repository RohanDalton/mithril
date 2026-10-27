run := "pixi run -e dev"
build_run := "pixi run -e build"

[doc("Automatically format files")]
format *args=".":
    {{ run }} ruff check --fix {{ args }}
    {{ run }} ruff format {{ args }}

[doc("Check linting and types without modifying files")]
lint *args=".":
    {{ run }} ruff check {{ args }}
    just --justfile {{justfile()}} typecheck {{ args }}

[doc("Run the test suite")]
test *args="tests":
    {{ run }} pytest {{ args }}

[doc("Typecheck the code")]
typecheck *args=".":
    {{ run }} ty check {{ args }}

[doc("Build the wheel and source distribution")]
build:
    {{ build_run }} python -m build
    {{ build_run }} python -m twine check --strict dist/*

[doc("Upload built distributions to PyPI")]
publish:
    {{ build_run }} python -m twine check --strict dist/*
    {{ build_run }} python -m twine upload dist/*
