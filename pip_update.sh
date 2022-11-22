#!/bin/bash

POSITIONAL_ARGS=()

pip3 uninstall featureform -y
rm -r client/dist/*

while [[ $# -gt 0 ]]; do
  case $1 in
    -d|--dashboard)
      shift # past argument
      shift # past value
      cd dashboard && npm run build
      cd ../
      mkdir -p client/src/featureform/dashboard/
      cp -r dashboard/out client/src/featureform/dashboard/
      ;;
    -*|--*)
      echo "Unknown option $1"
      exit 1
  esac
done

python3 -m build ./client/
pip3 install client/dist/*.whl