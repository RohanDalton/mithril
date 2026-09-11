run := "pixi run -e dev"

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
