#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import random
import re
import time
from abc import ABC, abstractmethod
from typing import Any, Iterable, Mapping, MutableMapping, Optional
from urllib.parse import unquote

import pendulum
import requests
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams import IncrementalMixin
from airbyte_cdk.sources.streams.core import StreamData
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.requests_native_auth import Oauth2Authenticator

class GoogleMyBusinessProfilePerformanceStream(HttpStream, ABC):

    url_base = "https://businessprofileperformance.googleapis.com/v1/"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        """
        TODO: Override this method to define any query parameters to be set. Remove this method if you don't need to define request params.
        Usually contains common params e.g. pagination size etc.
        """
        return {}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        TODO: Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        yield {}

class GoogleMyBusinessInformationStream(HttpStream, ABC):
    url_base = "https://mybusinessbusinessinformation.googleapis.com/v1/"

    def __init__(self, config: Mapping[str, Any]):
        self._accounts = config["accounts"]
        self._credentials = config["credentials"]

        self._state_per_account = {}
        for account in self._accounts:
            self._state_per_account[str(account)] = {}
    
        self._oauth2_authenticator = Oauth2Authenticator(
            token_refresh_endpoint="https://oauth2.googleapis.com/token",
            client_id=self._credentials['client_id'],
            client_secret=self._credentials['client_secret'],
            refresh_token=self._credentials['refresh_token'],
        )

        super().__init__(self._oauth2_authenticator)

    @property
    def state(self) -> MutableMapping[str, Any]:
        return self._state_per_account

    @state.setter
    def state(self, value: MutableMapping[str, Any]):
        if not value:
            return

        self._state_per_account = value

    def request_headers(self, **kwargs) -> MutableMapping[str, Any]:
        """
        Overridden to request JSON response (default for Exact is XML).
        """

        return {"Accept": "application/json"}

    def request_params(
        self, next_page_token: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, **kwargs
    ) -> MutableMapping[str, Any]:
        """
        The sync endpoints requires selection of fields to return. We use the configured catalog to make selection
        of fields we want to have.
        """
        
    def parse_response(
            self,
            response: requests.Response,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:
        # The response is a simple JSON whose schema matches our stream's schema exactly, 
        # so we just return a list containing the response
        return [response.json()]
    
    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        # The API does not offer pagination, 
        # so we return None to indicate there are no more pages in the response
        return None
    
    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        """Overridden to return a list of accounts to extract endpoints for."""

        return [{"account": x} for x in self._accounts]


class Locations(GoogleMyBusinessInformationStream):
    primary_key = "customer_id"

    def path(
        self, 
        stream_state: Mapping[str, Any] = None, 
        stream_slice: Mapping[str, Any] = None, 
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        account = str(stream_slice["account"])

        return f"accounts/{account}/locations"
    
    def request_params(self, next_page_token: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, **kwargs) -> MutableMapping[str, Any]:
        read_mask = {"readMask": "name,storeCode,regularHours,name,languageCode,title,phoneNumbers,categories,storefrontAddress,websiteUri,regularHours,specialHours,serviceArea,labels,adWordsLocationExtensions,latlng,openInfo,metadata,profile,relationshipData,moreHours,serviceItems"}
        return next_page_token or read_mask

    def parse_response(
        self,
        response: requests.Response,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:
        return [response.json()]

    def read_records(self, sync_mode: SyncMode, stream_slice: Mapping[str, Any] = None, **kwargs) -> Iterable[StreamData]:
        """Overridden to change the url_base based on the current account, and to keep track of the cursor."""

        account = str(stream_slice["account"])
        self._url_base = f"https://mybusinessbusinessinformation.googleapis.com/v1/accounts/{account}/locations"

        self.logger.info(f"Syncing account {account}...")

        # Reset state for full refresh
        if sync_mode == SyncMode.full_refresh:
            self._state_per_account[account] = {}

        for record in super().read_records(sync_mode=sync_mode, stream_slice=stream_slice, **kwargs):
            # Track the largest cursor value
            if self.cursor_field and sync_mode == SyncMode.incremental:
                cursor_value = record[self.cursor_field]
                current_cursor_value = self._state_per_account[account].get(self.cursor_field)
                current_cursor_value = cursor_value if not current_cursor_value else current_cursor_value

                if current_cursor_value:
                    self._state_per_account[account].update({self.cursor_field: max(cursor_value, current_cursor_value)})

            yield record

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        # The API does not offer pagination, 
        # so we return None to indicate there are no more pages in the response
        return None
