#!/bin/bash
pip3 uninstall featureform -y
rm -r client/dist/*
python3 -m build ./client/
pip3 install client/dist/*.whl
