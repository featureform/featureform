#!/bin/bash
# set to fail if any command fails
set -e

TESTING_DIRECTORY="$( cd "$(dirname "$0")"/ ; pwd -P )"

echo "Running the spark sql definition $TESTING_DIRECTORY/spark_sql_definition.py script"
featureform apply $TESTING_DIRECTORY/spark_sql_definition.py
python $TESTING_DIRECTORY/spark_serving.py
echo -e "Spark SQL Job Completed.\n\n"

echo "Running the spark df definition $TESTING_DIRECTORY/spark_df_definition.py script"
featureform apply $TESTING_DIRECTORY/spark_df_definition.py
python $TESTING_DIRECTORY/spark_serving.py
echo "Spark DF Job Completed."

echo "Running the k8s df definition $TESTING_DIRECTORY/k8s_df_definition.py script"
featureform apply $TESTING_DIRECTORY/k8s_df_definition.py
python $TESTING_DIRECTORY/serving.py
echo "K8s DF Job Completed."

echo "Running the k8s sql definition $TESTING_DIRECTORY/k8s_sql_definition.py script"
featureform apply $TESTING_DIRECTORY/k8s_sql_definition.py
python $TESTING_DIRECTORY/serving.py
echo "K8s SQL Job Completed."