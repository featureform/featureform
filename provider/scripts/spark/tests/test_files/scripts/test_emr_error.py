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
