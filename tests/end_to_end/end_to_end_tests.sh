#!/bin/bash
TESTING_DIRECTORY="$( cd "$(dirname "$0")"/ ; pwd -P )"
echo "Running the spark sql definition $TESTING_DIRECTORY/spark_sql_definition.py script"
featureform apply $TESTING_DIRECTORY/spark_sql_definition.py
python $TESTING_DIRECTORY/spark_serving.py
echo -e "Job Completed.\n\n"


echo "Running the spark sql definition $TESTING_DIRECTORY/spark_df_definition.py script"
featureform apply $TESTING_DIRECTORY/spark_df_definition.py
python $TESTING_DIRECTORY/spark_serving.py