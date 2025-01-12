#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from .source import SourceSurveycto

def run():
    source = SourceSurveycto()
    launch(source, sys.argv[1:])
