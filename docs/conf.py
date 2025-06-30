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
        release = "1.1.0"

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
html_title = "Transmog 1.1.0"
html_short_title = "Transmog 1.1.0"

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

# -- Code importing utility for documentation --------------------------------

# Path to the examples directory (relative to conf.py)
EXAMPLES_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "examples")
)


def extract_code_from_example(
    example_path, start_line=None, end_line=None, strip_docstring=False
):
    """Extract code snippet from an example file.

    Args:
        example_path: Path to the example file (relative to examples directory)
        start_line: Start line number (1-indexed, inclusive)
        end_line: End line number (1-indexed, inclusive)
        strip_docstring: Whether to strip the module docstring

    Returns:
        str: Extracted code snippet
    """
    full_path = os.path.join(EXAMPLES_DIR, example_path)

    if not os.path.exists(full_path):
        return f"Error: Example file not found: {example_path}"

    with open(full_path) as f:
        lines = f.readlines()

    if strip_docstring:
        # Find and remove the module docstring
        in_docstring = False
        docstring_end = 0

        for i, line in enumerate(lines):
            if line.strip().startswith('"""') or line.strip().startswith("'''"):
                if not in_docstring:
                    in_docstring = True
                    continue
                else:
                    docstring_end = i + 1
                    break
            elif in_docstring and (
                line.strip().endswith('"""') or line.strip().endswith("'''")
            ):
                docstring_end = i + 1
                break

        if docstring_end > 0:
            lines = lines[:0] + lines[docstring_end:]

    # Extract specified lines if provided
    if start_line is not None and end_line is not None:
        # Convert to 0-indexed
        start_idx = max(0, start_line - 1)
        end_idx = min(len(lines), end_line)
        lines = lines[start_idx:end_idx]

    return "".join(lines)


def setup_code_importing(app):
    """Set up code importing functionality."""
    app.add_config_value("examples_dir", EXAMPLES_DIR, "env")

    # Register a new directive for including example code
    from docutils import nodes
    from docutils.parsers.rst import Directive

    class IncludeExampleDirective(Directive):
        """Directive for including code from examples."""

        has_content = False
        required_arguments = 1
        optional_arguments = 0
        final_argument_whitespace = True
        option_spec = {
            "start-line": int,
            "end-line": int,
            "strip-docstring": lambda x: x.strip().lower() == "true",
            "language": str,
        }

        def run(self):
            example_path = self.arguments[0]
            start_line = self.options.get("start-line")
            end_line = self.options.get("end-line")
            strip_docstring = self.options.get("strip-docstring", False)
            language = self.options.get("language", "python")

            code = extract_code_from_example(
                example_path,
                start_line=start_line,
                end_line=end_line,
                strip_docstring=strip_docstring,
            )

            literal = nodes.literal_block(code, code)
            literal["language"] = language

            return [literal]

    app.add_directive("include-example", IncludeExampleDirective)


# Additional items for autodoc to skip
def skip_member(app, what, name, obj, skip, options):
    """Skip specific members based on criteria."""
    # Skip anything starting with underscore unless specifically included
    if name.startswith("_") and not name.startswith("__"):
        return True
    return None


def setup(app):
    """Set up Sphinx application."""
    app.connect("autodoc-skip-member", skip_member)

    # Set up code importing
    setup_code_importing(app)

    # Add CSS classes for admonitions
    app.add_css_file("css/custom.css")

    return {
        "version": release,
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }
