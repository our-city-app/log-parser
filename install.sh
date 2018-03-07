#!/usr/bin/env bash
virtualenv -p python3 venv
venv/bin/pip3 install -U -r requirements.txt
venv/bin/pip3 install -U -r test-requirements.txt