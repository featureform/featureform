#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

# this script is a no-op to be used for testing
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("Noop").getOrCreate()
    print("Doing nothing")


if __name__ == "__main__":
    main()
