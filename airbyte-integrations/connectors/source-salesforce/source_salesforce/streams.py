#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import csv
import ctypes
import math
import os
import time
import urllib.parse
from abc import ABC
from contextlib import closing
from typing import Any, Callable, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Type, Union

import pandas as pd
import pendulum
import requests  # type: ignore[import]
from airbyte_cdk.models import ConfiguredAirbyteCatalog, SyncMode
from airbyte_cdk.sources.streams.availability_strategy import AvailabilityStrategy
from airbyte_cdk.sources.streams.core import Stream, StreamData
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.utils.transform import TransformConfig, TypeTransformer
from numpy import nan
from pendulum import DateTime  # type: ignore[attr-defined]
from requests import codes, exceptions

from .api import UNSUPPORTED_FILTERING_STREAMS, Salesforce
from .availability_strategy import SalesforceAvailabilityStrategy
from .exceptions import SalesforceException, TmpFileIOError
from .rate_limiting import default_backoff_handler

# https://stackoverflow.com/a/54517228
CSV_FIELD_SIZE_LIMIT = int(ctypes.c_ulong(-1).value // 2)
csv.field_size_limit(CSV_FIELD_SIZE_LIMIT)

DEFAULT_ENCODING = "utf-8"


class SalesforceStream(HttpStream, ABC):
    page_size = 2000
    transformer = TypeTransformer(TransformConfig.DefaultSchemaNormalization)
    encoding = DEFAULT_ENCODING

    def __init__(
        self, sf_api: Salesforce, pk: str, stream_name: str, sobject_options: Mapping[str, Any] = None, schema: dict = None, **kwargs
    ):
        super().__init__(**kwargs)
        self.sf_api = sf_api
        self.pk = pk
        self.stream_name = stream_name
        self.schema: Mapping[str, Any] = schema  # type: ignore[assignment]
        self.sobject_options = sobject_options

    @property
    def max_properties_length(self) -> int:
        return Salesforce.REQUEST_SIZE_LIMITS - len(self.url_base) - 2000

    @property
    def name(self) -> str:
        return self.stream_name

    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        return self.pk

    @property
    def url_base(self) -> str:
        return self.sf_api.instance_url

    @property
    def availability_strategy(self) -> Optional["AvailabilityStrategy"]:
        return SalesforceAvailabilityStrategy()

    @property
    def too_many_properties(self):
        selected_properties = self.get_json_schema().get("properties", {})
        properties_length = len(urllib.parse.quote(",".join(p for p in selected_properties)))
        return properties_length > self.max_properties_length

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        yield from response.json()["records"]

    def get_json_schema(self) -> Mapping[str, Any]:
        if not self.schema:
            self.schema = self.sf_api.generate_schema(self.name)
        return self.schema

    def get_error_display_message(self, exception: BaseException) -> Optional[str]:
        if isinstance(exception, exceptions.ConnectionError):
            return f"After {self.max_retries} retries the connector has failed with a network error. It looks like Salesforce API experienced temporary instability, please try again later."
        return super().get_error_display_message(exception)


class RestSalesforceStream(SalesforceStream):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        assert self.primary_key or not self.too_many_properties

    def path(self, next_page_token: Mapping[str, Any] = None, **kwargs: Any) -> str:
        if next_page_token:
            """
            If `next_page_token` is set, subsequent requests use `nextRecordsUrl`.
            """
            next_token: str = next_page_token["next_token"]
            return next_token
        return f"/services/data/{self.sf_api.version}/queryAll"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        response_data = response.json()
        next_token = response_data.get("nextRecordsUrl")
        return {"next_token": next_token} if next_token else None

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
        property_chunk: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        """
        Salesforce SOQL Query: https://developer.salesforce.com/docs/atlas.en-us.232.0.api_rest.meta/api_rest/dome_queryall.htm
        """
        if next_page_token:
            """
            If `next_page_token` is set, subsequent requests use `nextRecordsUrl`, and do not include any parameters.
            """
            return {}

        property_chunk = property_chunk or {}
        query = f"SELECT {','.join(property_chunk.keys())} FROM {self.name} "

        if self.primary_key and self.name not in UNSUPPORTED_FILTERING_STREAMS:
            query += f"ORDER BY {self.primary_key} ASC"

        return {"q": query}

    def chunk_properties(self) -> Iterable[Mapping[str, Any]]:
        selected_properties = self.get_json_schema().get("properties", {})

        def empty_props_with_pk_if_present():
            return {self.primary_key: selected_properties[self.primary_key]} if self.primary_key else {}

        summary_length = 0
        local_properties = empty_props_with_pk_if_present()
        for property_name, value in selected_properties.items():
            current_property_length = len(urllib.parse.quote(f"{property_name},"))
            if current_property_length + summary_length >= self.max_properties_length:
                yield local_properties
                local_properties = empty_props_with_pk_if_present()
                summary_length = 0

            local_properties[property_name] = value
            summary_length += current_property_length

        if local_properties:
            yield local_properties

    def _read_pages(
        self,
        records_generator_fn: Callable[
            [requests.PreparedRequest, requests.Response, Mapping[str, Any], Mapping[str, Any]], Iterable[StreamData]
        ],
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[StreamData]:
        stream_state = stream_state or {}
        pagination_complete = False
        records = {}
        next_pages = {}

        while not pagination_complete:
            index = 0
            for index, property_chunk in enumerate(self.chunk_properties()):
                request, response = self._fetch_next_page(stream_slice, stream_state, next_pages.get(index), property_chunk)
                next_pages[index] = self.next_page_token(response)
                chunk_page_records = records_generator_fn(request, response, stream_state, stream_slice)
                if not self.too_many_properties:
                    # this is the case when a stream has no primary key
                    # (is allowed when properties length does not exceed the maximum value)
                    # so there would be a single iteration, therefore we may and should yield records immediately
                    yield from chunk_page_records
                    break
                chunk_page_records = {record[self.primary_key]: record for record in chunk_page_records}

                for record_id, record in chunk_page_records.items():
                    if record_id not in records:
                        records[record_id] = (record, 1)
                        continue
                    incomplete_record, counter = records[record_id]
                    incomplete_record.update(record)
                    counter += 1
                    records[record_id] = (incomplete_record, counter)

            for record_id, (record, counter) in records.items():
                if counter != index + 1:
                    # Because we make multiple calls to query N records (each call to fetch X properties of all the N records),
                    # there's a chance that the number of records corresponding to the query may change between the calls. This
                    # may result in data inconsistency. We skip such records for now and log a warning message.
                    self.logger.warning(
                        f"Inconsistent record with primary key {record_id} found. It consists of {counter} chunks instead of {index + 1}. "
                        f"Skipping it."
                    )
                    continue
                yield record

            records = {}

            if not any(next_pages.values()):
                pagination_complete = True

        # Always return an empty generator just in case no records were ever yielded
        yield from []

    def _fetch_next_page(
        self,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
        property_chunk: Mapping[str, Any] = None,
    ) -> Tuple[requests.PreparedRequest, requests.Response]:
        request_headers = self.request_headers(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token)
        request = self._create_prepared_request(
            path=self.path(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token),
            headers=dict(request_headers, **self.authenticator.get_auth_header()),
            params=self.request_params(
                stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token, property_chunk=property_chunk
            ),
            json=self.request_body_json(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token),
            data=self.request_body_data(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token),
        )
        request_kwargs = self.request_kwargs(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token)

        response = self._send_request(request, request_kwargs)
        return request, response


class BulkSalesforceStream(SalesforceStream):
    page_size = 15000
    DEFAULT_WAIT_TIMEOUT_SECONDS = 86400  # 24-hour bulk job running time
    MAX_CHECK_INTERVAL_SECONDS = 2.0
    MAX_RETRY_NUMBER = 3

    def path(self, next_page_token: Mapping[str, Any] = None, **kwargs: Any) -> str:
        return f"/services/data/{self.sf_api.version}/jobs/query"

    transformer = TypeTransformer(TransformConfig.CustomSchemaNormalization | TransformConfig.DefaultSchemaNormalization)

    @default_backoff_handler(max_tries=5, factor=15)
    def _send_http_request(self, method: str, url: str, json: dict = None, stream: bool = False):
        headers = self.authenticator.get_auth_header()
        response = self._session.request(method, url=url, headers=headers, json=json, stream=stream)
        if response.status_code not in [200, 204]:
            self.logger.error(f"error body: {response.text}, sobject options: {self.sobject_options}")
        response.raise_for_status()
        return response

    def create_stream_job(self, query: str, url: str) -> Optional[str]:
        """
        docs: https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/create_job.html
        """
        json = {"operation": "queryAll", "query": query, "contentType": "CSV", "columnDelimiter": "COMMA", "lineEnding": "LF"}
        try:
            response = self._send_http_request("POST", url, json=json)
            job_id: str = response.json()["id"]
            return job_id
        except exceptions.HTTPError as error:
            if error.response.status_code in [codes.FORBIDDEN, codes.BAD_REQUEST]:
                # A part of streams can't be used by BULK API. Every API version can have a custom list of
                # these sobjects. Another part of them can be generated dynamically. That's why we can't track
                # them preliminarily and there is only one way is to except error with necessary messages about
                # their limitations. Now we know about 3 different reasons of similar errors:
                # 1) some SaleForce sobjects(streams) is not supported by the BULK API simply (as is).
                # 2) Access to a sobject(stream) is not available
                # 3) sobject is not queryable. It means this sobject can't be called directly.
                #    We can call it as part of response from another sobject only.  E.g.:
                #        initial query: "Select Id, Subject from ActivityHistory" -> error
                #        updated query: "Select Name, (Select Subject,ActivityType from ActivityHistories) from Contact"
                #    The second variant forces customisation for every case (ActivityHistory, ActivityHistories etc).
                #    And the main problem is these subqueries doesn't support CSV response format.
                error_data = error.response.json()[0]
                error_code = error_data.get("errorCode")
                error_message = error_data.get("message", "")
                if error_message == "Selecting compound data not supported in Bulk Query" or (
                    error_code == "INVALIDENTITY" and "is not supported by the Bulk API" in error_message
                ):
                    self.logger.error(
                        f"Cannot receive data for stream '{self.name}' using BULK API, "
                        f"sobject options: {self.sobject_options}, error message: '{error_message}'"
                    )
                elif error.response.status_code == codes.FORBIDDEN and error_code != "REQUEST_LIMIT_EXCEEDED":
                    self.logger.error(
                        f"Cannot receive data for stream '{self.name}' ,"
                        f"sobject options: {self.sobject_options}, error message: '{error_message}'"
                    )
                elif error.response.status_code == codes.FORBIDDEN and error_code == "REQUEST_LIMIT_EXCEEDED":
                    self.logger.error(
                        f"Cannot receive data for stream '{self.name}' ,"
                        f"sobject options: {self.sobject_options}, Error message: '{error_data.get('message')}'"
                    )
                elif error.response.status_code == codes.BAD_REQUEST and error_message.endswith("does not support query"):
                    self.logger.error(
                        f"The stream '{self.name}' is not queryable, "
                        f"sobject options: {self.sobject_options}, error message: '{error_message}'"
                    )
                else:
                    raise error
            else:
                raise error
        return None

    def wait_for_job(self, url: str) -> str:
        expiration_time: DateTime = pendulum.now().add(seconds=self.DEFAULT_WAIT_TIMEOUT_SECONDS)
        job_status = "InProgress"
        delay_timeout = 0.0
        delay_cnt = 0
        job_info = None
        # minimal starting delay is 0.5 seconds.
        # this value was received empirically
        time.sleep(0.5)
        while pendulum.now() < expiration_time:
            job_info = self._send_http_request("GET", url=url).json()
            job_status = job_info["state"]
            if job_status in ["JobComplete", "Aborted", "Failed"]:
                if job_status != "JobComplete":
                    # this is only job metadata without payload
                    error_message = job_info.get("errorMessage")
                    if not error_message:
                        # not all failed response can have "errorMessage" and we need to show full response body
                        error_message = job_info
                    self.logger.error(f"JobStatus: {job_status}, sobject options: {self.sobject_options}, error message: '{error_message}'")

                return job_status

            if delay_timeout < self.MAX_CHECK_INTERVAL_SECONDS:
                delay_timeout = 0.5 + math.exp(delay_cnt) / 1000.0
                delay_cnt += 1

            time.sleep(delay_timeout)
            job_id = job_info["id"]
            self.logger.info(
                f"Sleeping {delay_timeout} seconds while waiting for Job: {self.name}/{job_id} to complete. Current state: {job_status}"
            )

        self.logger.warning(f"Not wait the {self.name} data for {self.DEFAULT_WAIT_TIMEOUT_SECONDS} seconds, data: {job_info}!!")
        return job_status

    def execute_job(self, query: str, url: str) -> Tuple[Optional[str], Optional[str]]:
        job_status = "Failed"
        for i in range(0, self.MAX_RETRY_NUMBER):
            job_id = self.create_stream_job(query=query, url=url)
            if not job_id:
                return None, job_status
            job_full_url = f"{url}/{job_id}"
            job_status = self.wait_for_job(url=job_full_url)
            if job_status not in ["UploadComplete", "InProgress"]:
                break
            self.logger.error(f"Waiting error. Try to run this job again {i + 1}/{self.MAX_RETRY_NUMBER}...")
            self.abort_job(url=job_full_url)
            job_status = "Aborted"

        if job_status in ["Aborted", "Failed"]:
            self.delete_job(url=job_full_url)
            return None, job_status
        return job_full_url, job_status

    def filter_null_bytes(self, b: bytes):
        """
        https://github.com/airbytehq/airbyte/issues/8300
        """
        res = b.replace(b"\x00", b"")
        if len(res) < len(b):
            self.logger.warning("Filter 'null' bytes from string, size reduced %d -> %d chars", len(b), len(res))
        return res

    def download_data(self, url: str, chunk_size: int = 1024) -> tuple[str, str]:
        """
        Retrieves binary data result from successfully `executed_job`, using chunks, to avoid local memory limitations.
        @ url: string - the url of the `executed_job`
        @ chunk_size: int - the buffer size for each chunk to fetch from stream, in bytes, default: 1024 bytes
        Return the tuple containing string with file path of downloaded binary data (Saved temporarily) and file encoding.
        """
        # set filepath for binary data from response
        tmp_file = os.path.realpath(os.path.basename(url))
        with closing(self._send_http_request("GET", f"{url}/results", stream=True)) as response, open(tmp_file, "wb") as data_file:
            response_encoding = response.apparent_encoding or response.encoding or self.encoding
            for chunk in response.iter_content(chunk_size=chunk_size):
                data_file.write(self.filter_null_bytes(chunk))
        # check the file exists
        if os.path.isfile(tmp_file):
            return tmp_file, response_encoding
        else:
            raise TmpFileIOError(f"The IO/Error occured while verifying binary data. Stream: {self.name}, file {tmp_file} doesn't exist.")

    def read_with_chunks(self, path: str, file_encoding: str, chunk_size: int = 100) -> Iterable[Tuple[int, Mapping[str, Any]]]:
        """
        Reads the downloaded binary data, using lines chunks, set by `chunk_size`.
        @ path: string - the path to the downloaded temporarily binary data.
        @ file_encoding: string - encoding for binary data file according to Standard Encodings from codecs module
        @ chunk_size: int - the number of lines to read at a time, default: 100 lines / time.
        """
        try:
            with open(path, "r", encoding=file_encoding) as data:
                chunks = pd.read_csv(data, chunksize=chunk_size, iterator=True, dialect="unix")
                for chunk in chunks:
                    chunk = chunk.replace({nan: None}).to_dict(orient="records")
                    for row in chunk:
                        yield row
        except pd.errors.EmptyDataError as e:
            self.logger.info(f"Empty data received. {e}")
            yield from []
        except IOError as ioe:
            raise TmpFileIOError(f"The IO/Error occured while reading tmp data. Called: {path}. Stream: {self.name}", ioe)
        finally:
            # remove binary tmp file, after data is read
            os.remove(path)

    def abort_job(self, url: str):
        data = {"state": "Aborted"}
        self._send_http_request("PATCH", url=url, json=data)
        self.logger.warning("Broken job was aborted")

    def delete_job(self, url: str):
        self._send_http_request("DELETE", url=url)

    @property
    def availability_strategy(self) -> Optional["AvailabilityStrategy"]:
        return None

    def next_page_token(self, last_record: Mapping[str, Any]) -> Optional[Mapping[str, Any]]:
        if self.primary_key and self.name not in UNSUPPORTED_FILTERING_STREAMS:
            return {"next_token": f"WHERE {self.primary_key} >= '{last_record[self.primary_key]}' "}  # type: ignore[index]
        return None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        """
        Salesforce SOQL Query: https://developer.salesforce.com/docs/atlas.en-us.232.0.api_rest.meta/api_rest/dome_queryall.htm
        """

        selected_properties = self.get_json_schema().get("properties", {})
        query = f"SELECT {','.join(selected_properties.keys())} FROM {self.name} "
        if next_page_token:
            query += next_page_token["next_token"]

        if self.primary_key and self.name not in UNSUPPORTED_FILTERING_STREAMS:
            query += f"ORDER BY {self.primary_key} ASC LIMIT {self.page_size}"
        return {"q": query}

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        stream_state = stream_state or {}
        next_page_token = None

        while True:
            params = self.request_params(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token)
            path = self.path(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token)
            job_full_url, job_status = self.execute_job(query=params["q"], url=f"{self.url_base}{path}")
            if not job_full_url:
                if job_status == "Failed":
                    # As rule as BULK logic returns unhandled error. For instance:
                    # error message: 'Unexpected exception encountered in query processing.
                    #                 Please contact support with the following id: 326566388-63578 (-436445966)'"
                    # Thus we can try to switch to GET sync request because its response returns obvious error message
                    standard_instance = self.get_standard_instance()
                    self.logger.warning("switch to STANDARD(non-BULK) sync. Because the SalesForce BULK job has returned a failed status")
                    stream_is_available, error = standard_instance.check_availability(self.logger, None)
                    if not stream_is_available:
                        self.logger.warning(f"Skipped syncing stream '{standard_instance.name}' because it was unavailable. Error: {error}")
                        return
                    yield from standard_instance.read_records(
                        sync_mode=sync_mode, cursor_field=cursor_field, stream_slice=stream_slice, stream_state=stream_state
                    )
                    return
                raise SalesforceException(f"Job for {self.name} stream using BULK API was failed.")

            count = 0
            record: Mapping[str, Any] = {}
            for record in self.read_with_chunks(*self.download_data(url=job_full_url)):
                count += 1
                yield record
            self.delete_job(url=job_full_url)

            if count < self.page_size:
                # Salesforce doesn't give a next token or something to know the request was
                # the last page. The connectors will sync batches in `page_size` and
                # considers that batch is smaller than the `page_size` it must be the last page.
                break

            next_page_token = self.next_page_token(record)
            if not next_page_token:
                # not found a next page data.
                break

    def get_standard_instance(self) -> SalesforceStream:
        """Returns a instance of standard logic(non-BULK) with same settings"""
        stream_kwargs = dict(
            sf_api=self.sf_api,
            pk=self.pk,
            stream_name=self.stream_name,
            schema=self.schema,
            sobject_options=self.sobject_options,
            authenticator=self.authenticator,
        )
        new_cls: Type[SalesforceStream] = RestSalesforceStream
        if isinstance(self, BulkIncrementalSalesforceStream):
            stream_kwargs.update({"replication_key": self.replication_key, "start_date": self.start_date})
            new_cls = IncrementalRestSalesforceStream

        return new_cls(**stream_kwargs)


@BulkSalesforceStream.transformer.registerCustomTransform
def transform_empty_string_to_none(instance: Any, schema: Any):
    """
    BULK API returns a `csv` file, where all values are initially as string type.
    This custom transformer replaces empty lines with `None` value.
    """
    if isinstance(instance, str) and not instance.strip():
        instance = None

    return instance


class IncrementalRestSalesforceStream(RestSalesforceStream, ABC):
    state_checkpoint_interval = 500

    def __init__(self, replication_key: str, start_date: Optional[str], **kwargs):
        super().__init__(**kwargs)
        self.replication_key = replication_key
        self.start_date = self.format_start_date(start_date)

    @staticmethod
    def format_start_date(start_date: Optional[str]) -> Optional[str]:
        """Transform the format `2021-07-25` into the format `2021-07-25T00:00:00Z`"""
        if start_date:
            return pendulum.parse(start_date).strftime("%Y-%m-%dT%H:%M:%SZ")  # type: ignore[attr-defined,no-any-return]
        return None

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
        property_chunk: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        if next_page_token:
            """
            If `next_page_token` is set, subsequent requests use `nextRecordsUrl`, and do not include any parameters.
            """
            return {}

        property_chunk = property_chunk or {}

        stream_date = stream_state.get(self.cursor_field)
        start_date = stream_date or self.start_date

        query = f"SELECT {','.join(property_chunk.keys())} FROM {self.name} "
        if start_date:
            query += f"WHERE {self.cursor_field} >= {start_date} "
        if self.name not in UNSUPPORTED_FILTERING_STREAMS:
            query += f"ORDER BY {self.cursor_field} ASC"
        return {"q": query}

    @property
    def cursor_field(self) -> str:
        return self.replication_key

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Return the latest state by comparing the cursor value in the latest record with the stream's most recent state
        object and returning an updated state object.
        """
        latest_benchmark = latest_record[self.cursor_field]
        if current_stream_state.get(self.cursor_field):
            return {self.cursor_field: max(latest_benchmark, current_stream_state[self.cursor_field])}
        return {self.cursor_field: latest_benchmark}


class BulkIncrementalSalesforceStream(BulkSalesforceStream, IncrementalRestSalesforceStream):
    def next_page_token(self, last_record: Mapping[str, Any]) -> Optional[Mapping[str, Any]]:
        if self.name not in UNSUPPORTED_FILTERING_STREAMS:
            page_token: str = last_record[self.cursor_field]
            res = {"next_token": page_token}
            # use primary key as additional filtering param, if cursor_field is not increased from previous page
            if self.primary_key and self.prev_start_date == page_token:
                res["primary_key"] = last_record[self.primary_key]
            return res
        return None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        selected_properties = self.get_json_schema().get("properties", {})

        stream_date = stream_state.get(self.cursor_field)
        next_token = (next_page_token or {}).get("next_token")
        primary_key = (next_page_token or {}).get("primary_key")
        start_date = next_token or stream_date or self.start_date
        self.prev_start_date = start_date

        query = f"SELECT {','.join(selected_properties.keys())} FROM {self.name} "
        if start_date:
            if primary_key and self.name not in UNSUPPORTED_FILTERING_STREAMS:
                query += f"WHERE ({self.cursor_field} = {start_date} AND {self.primary_key} > '{primary_key}') OR ({self.cursor_field} > {start_date}) "
            else:
                query += f"WHERE {self.cursor_field} >= {start_date} "
        if self.name not in UNSUPPORTED_FILTERING_STREAMS:
            order_by_fields = [self.cursor_field, self.primary_key] if self.primary_key else [self.cursor_field]
            query += f"ORDER BY {','.join(order_by_fields)} ASC LIMIT {self.page_size}"
        return {"q": query}


class Describe(Stream):
    """
    Stream of sObjects' (Salesforce Objects) describe:
    https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_sobject_describe.htm
    """

    name = "Describe"
    primary_key = "name"

    def __init__(self, sf_api: Salesforce, catalog: ConfiguredAirbyteCatalog = None, **kwargs):
        super().__init__(**kwargs)
        self.sf_api = sf_api
        if catalog:
            self.sobjects_to_describe = [s.stream.name for s in catalog.streams if s.stream.name != self.name]

    def read_records(self, **kwargs) -> Iterable[Mapping[str, Any]]:
        """
        Yield describe response of SObjects defined in catalog as streams only.
        """
        for sobject in self.sobjects_to_describe:
            yield self.sf_api.describe(sobject=sobject)
