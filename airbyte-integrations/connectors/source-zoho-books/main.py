#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_zoho_books import SourceZohoBooks

if __name__ == "__main__":
    source = SourceZohoBooks()
    launch(source, sys.argv[1:])
