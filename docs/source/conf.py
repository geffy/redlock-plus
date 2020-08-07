# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys
sys.path.insert(0, os.path.abspath('../redlock_plus.py'))
import redlock_plus


# -- Project information -----------------------------------------------------

project = 'redlock-plus'
copyright = '2020, Janek Nouvertné'
author = 'Janek Nouvertné'

# The full version, including alpha/beta/rc tags
release = redlock_plus.__version__


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.intersphinx",
    "sphinx_autodoc_typehints",
]

autodoc_member_order = "bysource"


intersphinx_mapping = {
    'python': ('https://docs.python.org/3', None),
    'redis': ('https://redis-py.readthedocs.io/en/stable/', None)
}

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'alabaster'
html_theme_options = {
    "github_user": "provinzkraut",
    "github_repo": "redlock-plus",
    "github_button": False,
    "github_banner": True,
    "extra_nav_links": {
        "Code": "https://github.com/provinzkraut/redlock-plus",
        "Issues": "https://github.com/provinzkraut/redlock-plus/issues",
    },
    "fixed_sidebar": True
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']

html_sidebars = {
    '**': [
        'about.html',
        'navigation.html',
        'searchbox.html',
    ]
}
