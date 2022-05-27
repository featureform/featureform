#!/bin/bash
pip uninstall featureform -y
rm -r client/dist/*
python3 -m build ./client/
pip install client/dist/*.whl
