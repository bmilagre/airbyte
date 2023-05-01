#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_google_my_business import SourceGoogleMyBusiness

if __name__ == "__main__":
    source = SourceGoogleMyBusiness()
    launch(source, sys.argv[1:])
