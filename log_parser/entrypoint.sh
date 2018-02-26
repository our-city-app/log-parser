#!/usr/bin/env bash
export PYTHONPATH=${PYTHONPATH}:/code
echo ${PYTHONPATH}
python3 /code/log_parser/__init__.py
