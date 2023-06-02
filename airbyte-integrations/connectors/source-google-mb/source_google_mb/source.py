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
from datetime import date, timedelta


# Source
class SourceGoogleMb(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        input_location = int(config['location_id'])
        number_of_days = config['date_range']['number_of_days'] if "number_of_days" in config['date_range'] else None
        start_date = config['date_range']['start_date'] if "start_date" in config['date_range'] else None
        end_date = config['date_range']['end_date'] if "end_date" in config['date_range'] else None

        if not input_location:
            return False, f"Input Location not provided. Check your configuration"

        if not number_of_days and (not start_date and not end_date):
            return False, f"Number of days or start_date and end_date needs to be filled"

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

        input_location = int(config['location_id'])
        number_of_days = config['date_range']['number_of_days'] if "number_of_days" in config['date_range'] else None
        
        if number_of_days:
            start_date, end_date = self.build_date_range(number_of_days)
        else:
            start_date = config['date_range']['start_date']
            end_date = config['date_range']['end_date']

        print(start_date, end_date)

        authenticator = Oauth2Authenticator(
            token_refresh_endpoint="https://oauth2.googleapis.com/token",
            client_id=creds['client_id'],
            client_secret=creds['client_secret'],
            refresh_token=creds['refresh_token'],
            scopes=['https://www.googleapis.com/auth/business.manage']
        )

        args = {
            "authenticator": authenticator,
            "location_id": input_location,
            "start_date": start_date,
            "end_date": end_date,
        }

        return [Location(**args)]

    @staticmethod
    def build_date_range(number_of_days):
        date_format = '%Y-%m-%d'
        start_date = (date.today() - timedelta(days=number_of_days)).strftime(date_format)
        end_date = (date.today()).strftime(date_format)
        return [start_date, end_date]
