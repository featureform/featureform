#!/bin/bash

./gen_grpc.sh
python3 -m twine upload --repository featureform dist/*