#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

# this script is used to test AWS EMR execution
# and our ability to read the error when it fails

import sys


def main():
    if len(sys.argv) == 2:
        raise Exception(f"ERROR: {sys.argv[1]}")
    else:
        raise Exception("There is an Error")


if __name__ == "__main__":
    main()
