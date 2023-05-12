#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
import traceback
from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from .streams import Location
from airbyte_cdk.sources.streams.http.requests_native_auth import Oauth2Authenticator


# Source
class SourceGoogleMb(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        input_location = config['location_id']

        if not input_location:
            return False, f"Input Location not provided. Check your configuration"
        else:

            try:
                ''' Implement check logic '''
                return True, None
            except Exception as exception:
                logger.error(traceback.format_exc())
                return False, f"Unable to connect - {exception}"

    @staticmethod
    def get_credentials(config: Mapping[str, Any]) -> MutableMapping[str, Any]:
        credentials = config["credentials"]

        return credentials

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:

        creds = self.get_credentials(config)

        authenticator = Oauth2Authenticator(
            token_refresh_endpoint="https://oauth2.googleapis.com/token",
            client_id=creds['client_id'],
            client_secret=creds['client_secret'],
            refresh_token=creds['refresh_token'],
            scopes=['https://www.googleapis.com/auth/business.manage']
        )

        args = {
            "authenticator": authenticator,
            "location_id": config["location_id"],
            "start_date": config["start_date"],
            "end_date": config["end_date"],
        }

        return [Location(**args)]
