#!/usr/bin/env bash
mypy log_parser/__init__.py --ignore-missing-import
venv/bin/python -m "nose"