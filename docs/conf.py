"""Sphinx configuration for Transmog documentation."""

import datetime
import os
import sys

# Add the project root directory to the path so we can import the package
sys.path.insert(0, os.path.abspath(".."))

# -- Project information -----------------------------------------------------
project = "Transmog"
author = "Scott Draper"
copyright_text = f"{datetime.datetime.now().year}, {author}"

# The full version, including alpha/beta/rc tags
# Import the version from the package
try:
    from transmog import __version__ as release
except ImportError:
    try:
        from src.transmog import __version__ as release
    except ImportError:
        release = "1.0.4"

# -- General configuration ---------------------------------------------------
extensions = [
    # Core Sphinx extensions
    "sphinx.ext.autodoc",  # Generate API documentation from docstrings
    "sphinx.ext.autosummary",  # Generate summary tables
    "sphinx.ext.viewcode",  # Add links to view source code
    "sphinx.ext.napoleon",  # Support for NumPy and Google style docstrings
    "sphinx.ext.intersphinx",  # Link to other project's documentation
    "sphinx.ext.todo",  # Support for TODO items
    "sphinx.ext.coverage",  # Documentation coverage checking
    "sphinx.ext.mathjax",  # Math rendering
    # Additional extensions
    "sphinx_autodoc_typehints",  # Better type hint support
    "myst_parser",  # Markdown support
    "sphinx_copybutton",  # Add copy button to code blocks
    "sphinx_design",  # Enhanced styling components
    "sphinxcontrib.mermaid",  # Mermaid diagram support
    "furo.sphinxext",  # Furo theme extension
]

# Configure MyST-Parser
myst_enable_extensions = [
    "amsmath",  # LaTex math
    "attrs_inline",  # Attributes in HTML
    "colon_fence",  # Fenced code blocks with colons
    "deflist",  # Definition lists
    "dollarmath",  # Inline math with $
    "fieldlist",  # Field lists
    "html_admonition",  # HTML admonitions
    "html_image",  # HTML images
    "linkify",  # Auto-convert URLs to links
    "replacements",  # Text replacements
    "smartquotes",  # Smart quotes
    "strikethrough",  # Strikethrough
    "substitution",  # Substitutions
    "tasklist",  # Task lists
]

# Configure autodoc
autodoc_default_options = {
    "members": True,
    "member-order": "bysource",
    "special-members": False,
    "undoc-members": True,
    "exclude-members": "__weakref__, __dict__, __module__, __annotations__",
    "show-inheritance": True,
    "inherited-members": False,
    "ignore-module-all": False,
    "private-members": False,  # Don't document private methods/attrs
}

# Turn on Napoleon (Google style docstrings)
napoleon_google_docstring = True
napoleon_numpy_docstring = False
napoleon_include_init_with_doc = True
napoleon_include_private_with_doc = False
napoleon_include_special_with_doc = False
napoleon_use_admonition_for_examples = True
napoleon_use_admonition_for_notes = True
napoleon_use_admonition_for_references = True

# Intersphinx mapping to other projects
intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "numpy": ("https://numpy.org/doc/stable/", None),
}

# Show todos in the documentation
todo_include_todos = True

# Configure MyST link types
myst_url_schemes = ["http", "https", "mailto"]

# Mermaid configuration
mermaid_init_js = """
    mermaid.initialize({
        startOnLoad: true,
        theme: "neutral",
        flowchart: { useMaxWidth: true },
        sequence: { useMaxWidth: true },
    });
"""

# -- Options for HTML output -------------------------------------------------
html_theme = "furo"
html_static_path = ["_static"]
html_css_files = ["css/custom.css"]
html_js_files = ["js/custom.js"]

# Theme options
html_theme_options = {
    # Light and dark mode
    "light_css_variables": {
        "color-brand-primary": "#2980B9",
        "color-brand-content": "#2980B9",
        "color-sidebar-background": "#f8f9fb",
        "color-background-primary": "#ffffff",
        "color-background-secondary": "#f8f9fb",
        "content-width": "1000px",
    },
    "dark_css_variables": {
        "color-brand-primary": "#58a6ff",
        "color-brand-content": "#58a6ff",
        "color-sidebar-background": "#1a1d21",
        "color-background-primary": "#0d1117",
        "color-background-secondary": "#161b22",
        "content-width": "1000px",
    },
    "sidebar_hide_name": False,
    "navigation_with_keys": True,
    "footer_icons": [],
}

# -- Options for linkcheck builder -------------------------------------------
# Add all known domains to the linkcheck_ignore list
linkcheck_ignore = [
    "http://localhost",  # Local development links
]
linkcheck_timeout = 30

# -- Options for HTMLHelp output ---------------------------------------------
htmlhelp_basename = "TransmogDoc"

# -- Options for LaTeX output ------------------------------------------------
latex_elements = {
    "papersize": "letterpaper",
    "pointsize": "10pt",
}

# Grouping the document tree into LaTeX files
latex_documents = [
    (
        "index",  # Source start file
        "Transmog.tex",  # Target file
        "Transmog Documentation",  # Title
        "Scott Draper",  # Author
        "manual",  # Document class
    ),
]

# -- Options for manual page output ------------------------------------------
man_pages = [
    (
        "index",  # Source start file
        "transmog",  # Name
        "Transmog Documentation",  # Title
        ["Scott Draper"],  # Authors
        1,  # Manual section
    ),
]

# -- Options for Texinfo output ----------------------------------------------
texinfo_documents = [
    (
        "index",  # Source start file
        "Transmog",  # Target name
        "Transmog Documentation",  # Title
        "Scott Draper",  # Author
        "Transmog",  # Dir menu entry
        "JSON transformation library",  # Description
        "Miscellaneous",  # Category
    ),
]

# -- Extension configuration -------------------------------------------------

# Automatically generate stub pages for autosummary
autosummary_generate = True

# Sort members by source order (matches code order)
autodoc_member_order = "bysource"

# Typehints settings
autodoc_typehints = "description"
autodoc_typehints_format = "short"

# Path setup for custom templates
templates_path = ["_templates"]


# Additional items for autodoc to skip
def skip_member(app, what, name, obj, skip, options):
    """Skip specific members based on criteria."""
    # Skip anything starting with underscore unless specifically included
    if name.startswith("_") and not name.startswith("__"):
        return True
    return None


def setup(app):
    """Setup custom configuration for Sphinx app."""
    # Register custom skip function
    app.connect("autodoc-skip-member", skip_member)

    # Add custom CSS
    app.add_css_file("css/custom.css")

    # Add the consolidated examples path for Sphinx to find
    app.config.html_context = {"examples_path": "../examples"}
