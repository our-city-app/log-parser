#!/usr/bin/env bash
mypy log_parser/__init__.py --ignore-missing-import

if [ -z ${CI+x} ]; then
    venv/bin/python -m "nose"
else
    python3 -m "nose"
fi
