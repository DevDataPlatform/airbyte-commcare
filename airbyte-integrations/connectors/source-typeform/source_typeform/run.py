#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from source_typeform import SourceTypeform

from airbyte_cdk.entrypoint import launch


def run():
    source = SourceTypeform()
    launch(source, sys.argv[1:])
