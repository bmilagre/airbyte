import requests
from typing import Optional, Mapping, Any, Iterable, MutableMapping
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.requests_native_auth import Oauth2Authenticator


class Location(HttpStream):
    url_base = "https://businessprofileperformance.googleapis.com/"

    primary_key = None
    http_method = "GET"
    dailyMetrics = [
        "BUSINESS_IMPRESSIONS_DESKTOP_MAPS",
        "BUSINESS_IMPRESSIONS_DESKTOP_SEARCH",
        "BUSINESS_IMPRESSIONS_MOBILE_MAPS",
        "BUSINESS_IMPRESSIONS_MOBILE_SEARCH",
        "BUSINESS_CONVERSATIONS",
        "BUSINESS_DIRECTION_REQUESTS",
        "CALL_CLICKS",
        "WEBSITE_CLICKS",
        "BUSINESS_BOOKINGS",
        "BUSINESS_FOOD_ORDERS",
        "BUSINESS_FOOD_MENU_CLICKS"
    ]

    def __init__(
            self,
            authenticator: Oauth2Authenticator,
            location_id: int,
            start_date: str,
            end_date: str
    ):
        self.location_id = location_id
        self.start_date = start_date
        self.end_date = end_date
        self._stop_iteration = False

        super().__init__(authenticator)

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def path(self, **kwargs) -> str:
        location_id = self.location_id

        return f"v1/locations/{location_id}:fetchMultiDailyMetricsTimeSeries"

    @staticmethod
    def _date_string_to_map(date: str) -> Mapping[str, str]:
        return {
            "year": date.split("-")[0],
            "month": date.split("-")[1],
            "day": date.split("-")[2]
        }

    def request_params(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:

        start_range = self._date_string_to_map(self.start_date)
        end_range = self._date_string_to_map(self.end_date)
        daily_metrics = self.dailyMetrics

        return {
            "daily_range.start_date.year": start_range["year"],
            "daily_range.start_date.month": start_range["month"],
            "daily_range.start_date.day": start_range["day"],
            "daily_range.end_date.year": end_range["year"],
            "daily_range.end_date.month": end_range["month"],
            "daily_range.end_date.day": end_range["day"],
            "dailyMetrics": daily_metrics
        }

    def read_records(self, **kwargs) -> Iterable[Mapping[str, Any]]:
        try:
            yield from super().read_records(**kwargs)
        except requests.exceptions.HTTPError as e:
            self._stop_iteration = True
            self.logger.error(e.response)
            if e.response.status_code != 429:
                raise e

    def parse_response(
            self,
            response: requests.Response,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None
    ) -> Iterable[Mapping]:
        data = response.json()

        print(data)
        result = {
            "records": []
        }

        for metric in data['multiDailyMetricTimeSeries'][0]['dailyMetricTimeSeries']:
            metric_name = metric['dailyMetric']
            for day_entry in metric['timeSeries']['datedValues']:
                year = str(day_entry['date']['year'])
                month = "{:02d}".format(day_entry['date']['month'])
                day = "{:02d}".format(day_entry['date']['day'])

                date = f"{year}-{month}-{day}"
                value = day_entry["value"] if "value" in day_entry else None

                if value is None:
                    continue

                record = {
                    "type": metric_name,
                    "value": value,
                    "date": date,
                    "location_id": str(self.location_id)
                }

                result["records"].append(record)
        print(result['records'])
        yield result
