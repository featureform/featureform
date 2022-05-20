#!/bin/bash
pip uninstall featureform -y
rm -r client/dist/*
python3 -m build ./client/
pip install client/dist/featureform-0.0.21-py3-none-any.whl
