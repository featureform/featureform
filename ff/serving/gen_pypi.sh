#!/bin/bash


./gen_grpc.sh
rm ./client/dist/*
set -e
python3 -m build ./client/
python3 -m twine upload -r pypi client/dist/*