#!/bin/bash
# $1 = type
# $2 = env
# $3 = version
if [ "$1" == "release" ]; then
  echo "TAG=$3" >> $2
elif [ "$1" == "pre-release" ]; then
  echo "TAG=$3-rc" >> $2
else
  echo "Invalid Version"
  exit 1
fi