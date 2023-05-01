#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from typing import Any, List, Mapping, Tuple

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from source_google_my_business.streams import (
    Locations
)

# Source
class SourceGoogleMyBusiness(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        refresh_token = (config or {}).get("credentials", {}).get("refresh_token")
        accounts = (config or {}).get("accounts", [])

        if not refresh_token:
            return False, "Missing refresh token"
        if not accounts:
            return False, "Missing accounts"
    
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
       return [
           Locations(config)
       ]