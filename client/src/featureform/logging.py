#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import logging
import sys


def setup_logging(debug=False):
    # This all is a bit of hack and should be replaced with a proper logging configuration

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG if debug else logging.WARNING)

    # Clear existing handlers to prevent duplicate logs
    while logger.handlers:
        logger.handlers.pop()

    # Define a common formatter
    formatter = logging.Formatter(
        "%(name)s: %(asctime)s | %(levelname)s | %(filename)s:%(lineno)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    # Add StreamHandler for console output in debug mode
    if debug:
        debug_handler = logging.StreamHandler(sys.stdout)
        debug_handler.setLevel(logging.DEBUG)
        debug_handler.setFormatter(formatter)
        logger.addHandler(debug_handler)

    # Always add a StreamHandler for warnings and above
    warning_handler = logging.StreamHandler(sys.stdout)
    warning_handler.setLevel(logging.WARNING)
    warning_handler.setFormatter(formatter)
    logger.addHandler(warning_handler)
