#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_vowel_lms import SourceVowelLms

if __name__ == "__main__":
    source = SourceVowelLms()
    launch(source, sys.argv[1:])
