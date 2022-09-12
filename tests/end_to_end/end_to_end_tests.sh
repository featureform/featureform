#!/bin/bash
TESTING_DIRECTORY="$( cd "$(dirname "$0")"/ ; pwd -P )"
echo "Running the spark definition $TESTING_DIRECTORY/spark_definition.py script"
featureform apply $TESTING_DIRECTORY/spark_definition.py

echo "Waiting 4 minutes for the jobs to finish"
sleep 180
python $TESTING_DIRECTORY/spark_serving.py
