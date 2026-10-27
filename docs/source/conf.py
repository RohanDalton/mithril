from pathlib import Path
from runpy import run_path

__author__ = "Rohan B. Dalton"

project = "Mithril"
copyright = "2026, Rohan B. Dalton"
author = "Rohan B. Dalton"
release = run_path(str(Path(__file__).resolve().parents[2] / "mithril" / "__version__.py"))[
    "__version__"
]

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = ["sphinx.ext.autodoc"]

# Keep the reference readable; lifecycle and type semantics are explained in
# api.rst rather than generating links for every dependency's annotation.
autodoc_typehints = "none"

templates_path = ["_templates"]
exclude_patterns = []


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "alabaster"
html_static_path = ["_static"]

if __name__ == "__main__":
    pass
