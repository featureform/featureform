#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

# To properly display pending deprecation warnings for the ResourceClient
# and ServingClient classes, we need to set the formatwarning function
# to a custom function to avoid the default behavior of printing the
# the absolute path to the file where the warning was raised. Additionally,
# we need to set the default filter to 'default' to avoid the default
# behavior of ignoring all warnings.
# TODO: Remove this code once the ResourceClient and ServingClient classes
# are deprecated.
import warnings


def custom_warning_formatter(message, category, filename, lineno, file=None, line=None):
    return f"{category.__name__}: {message}\n"


warnings.formatwarning = custom_warning_formatter
warnings.simplefilter("default")

from .register import *
from .serving import ServingClient
from .resources import (
    DatabricksCredentials,
    EMRCredentials,
    AWSStaticCredentials,
    AWSAssumeRoleCredentials,
    GCPCredentials,
    GlueCatalog,
    SparkCredentials,
    BasicCredentials,
    KerberosCredentials,
)
from .client import Client
from .enums import ResourceType, TableFormat, DataResourceType

ServingClient = ServingClient
ResourceClient = ResourceClient
Client = Client

# Executor Credentials
DatabricksCredentials = DatabricksCredentials
EMRCredentials = EMRCredentials
SparkCredentials = SparkCredentials

# Cloud Provider Credentials
AWSStaticCredentials = AWSStaticCredentials
AWSAssumeRoleCredentials = AWSAssumeRoleCredentials
GCPCredentials = GCPCredentials
GlueCatalog = GlueCatalog

# HDFS Credentials
BasicCredentials = BasicCredentials
KerberosCredentials = KerberosCredentials

# Class API
Feature = FeatureColumnResource
Label = LabelColumnResource
Variants = Variants
Embedding = EmbeddingColumnResource
FeatureStream = FeatureStreamResource
LabelStream = LabelStreamResource
MultiFeature = MultiFeatureColumnResource

set_run = set_run
get_run = get_run

# Enums
DataResourceType = DataResourceType
ResourceType = ResourceType
TableFormat = TableFormat
