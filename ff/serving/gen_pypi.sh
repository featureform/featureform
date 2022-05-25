#!/bin/bash

./gen_grpc.sh
rm ./client/dist/*
python3 -m build ./client/
python3 -m twine upload -r pypi client/dist/*