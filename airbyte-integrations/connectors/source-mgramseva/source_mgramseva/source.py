#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import base64
import hashlib
from datetime import datetime, timedelta
from logging import Logger
from dateutil.relativedelta import relativedelta
import requests
import pytz
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.core import StreamData

pytz.IST = pytz.timezone("Asia/Kolkata")


def convert_to_date(x: int) -> datetime:
    """convert a timestamp to a date"""
    return datetime.fromtimestamp(x / 1000, pytz.UTC).astimezone(pytz.IST)


# Basic full refresh stream
class MgramsevaStream(HttpStream, ABC):
    """Base for all objects"""

    url_base = "https://www.peyjalbihar.org/"

    http_method = "POST"

    primary_key = "id"

    def __init__(self, endpoint: str, headers: dict, request_info: dict, user_request: dict, params: dict, response_key: str, **kwargs):
        """set base url, headers, request info and user request"""
        super().__init__(**kwargs)
        self.endpoint = endpoint
        self.headers = headers
        self.request_info = request_info
        self.user_request = user_request
        self.params = params
        self.response_key = response_key

    def path(
        self,
        stream_state: Mapping[str, Any] = None,  # pylint: disable=unused-argument
        stream_slice: Mapping[str, Any] = None,  # pylint: disable=unused-argument
        next_page_token: Mapping[str, Any] = None,  # pylint: disable=unused-argument
    ) -> str:
        """path"""
        return self.endpoint

    def request_headers(
        self,
        stream_state: Optional[Mapping[str, Any]],  # pylint: disable=unused-argument
        stream_slice: Optional[Mapping[str, Any]] = None,  # pylint: disable=unused-argument
        next_page_token: Optional[Mapping[str, Any]] = None,  # pylint: disable=unused-argument
    ) -> Mapping[str, Any]:
        """Return headers required for the request"""
        return self.headers

    def request_body_json(
        self,
        stream_state: Optional[Mapping[str, Any]],  # pylint: disable=unused-argument
        stream_slice: Optional[Mapping[str, Any]] = None,  # pylint: disable=unused-argument
        next_page_token: Optional[Mapping[str, Any]] = None,  # pylint: disable=unused-argument
    ) -> Optional[Mapping[str, Any]]:
        """
        All requests require the same body
        """
        return {"RequestInfo": self.request_info, "userInfo": self.user_request}

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        TODO: Override this method to define a pagination strategy. If you will not be using pagination, no action is required - just return None.

        This method should return a Mapping (e.g: dict) containing whatever information required to make paginated requests. This dict is passed
        to most other methods in this class to help you form headers, request bodies, query params, etc..

        For example, if the API accepts a 'page' parameter to determine which page of the result to return, and a response from the API contains a
        'page' number, then this method should probably return a dict {'page': response.json()['page'] + 1} to increment the page count by 1.
        The request_params method should then read the input next_page_token and set the 'page' param to next_page_token['page'].

        :param response: the most recent response from the API
        :return If there is another page in the result, a mapping (e.g: dict) containing information needed to query the next page in the response.
                If there are no more pages in the result, return None.
        """
        return None

    def request_params(
        self,
        stream_state: Mapping[str, Any],  # pylint: disable=unused-argument
        stream_slice: Mapping[str, any] = None,  # pylint: disable=unused-argument
        next_page_token: Mapping[str, Any] = None,  # pylint: disable=unused-argument
    ) -> MutableMapping[str, Any]:
        """request parameters"""
        return self.params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        :return an iterable containing each record in the response
        """
        # self.logger.info(response.json())
        return map(lambda x: {"data": x, "id": x["id"]}, response.json()[self.response_key])


class MgramsevaDemands(MgramsevaStream):
    """object for consumer demands"""

    def __init__(
        self, headers: dict, request_info: dict, user_request: dict, tenantid_list: list, **kwargs
    ):  # pylint: disable=super-init-not-called
        """ctor"""
        self.tenantid_list = tenantid_list
        self.headers = headers
        self.request_info = request_info
        self.user_request = user_request
        self.response_key = "Demands"

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[StreamData]:
        """override"""
        for tenantid in self.tenantid_list:
            params = {
                "tenantId": tenantid,
                "businessService": "WS",
            }
            demandstream = MgramsevaStream(
                "billing-service/demand/_search", self.headers, self.request_info, self.user_request, params, self.response_key
            )
            yield from demandstream.read_records(sync_mode, cursor_field, stream_slice, stream_state)

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """include the bill date"""
        demands = response.json()[self.response_key]
        for demand in demands:
            demand["demandFromDate"] = convert_to_date(demand["taxPeriodFrom"]).strftime("%Y-%m-%d")
            demand["demandToDate"] = convert_to_date(demand["taxPeriodTo"]).strftime("%Y-%m-%d")
        return map(lambda x: {"data": x, "id": x["id"]}, demands)


class MgramsevaBills(MgramsevaStream):
    """object for consumer bills"""

    def __init__(
        self, headers: dict, request_info: dict, user_request: dict, tenantid_list: list, consumer_codes: dict, **kwargs
    ):  # pylint: disable=super-init-not-called
        """specify endpoint for bills and call super"""
        self.headers = headers
        self.request_info = request_info
        self.user_request = user_request
        self.consumer_codes = consumer_codes
        self.tenantid_list = tenantid_list

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[StreamData]:
        """override"""
        for tenantid in self.tenantid_list:
            for consumer_code in self.consumer_codes[tenantid]:
                params = {"tenantId": tenantid, "businessService": "WS", "consumerCode": consumer_code}
                consumer_code_stream = MgramsevaStream(
                    "billing-service/bill/v2/_fetchbill", self.headers, self.request_info, self.user_request, params, "Bill"
                )
                yield from consumer_code_stream.read_records(sync_mode, cursor_field, stream_slice, stream_state)


class MgramsevaTenantExpense(MgramsevaStream):
    """object for a single tenant expense"""

    def __init__(
        self,
        endpoint: str,
        headers: dict,
        request_info: dict,
        user_request: dict,
        tenantid: str,
        month_start: datetime,
        month_end: datetime,
        response_key: str,
        **kwargs,
    ):
        """call super"""
        self.tenantid = tenantid
        self.month_start = month_start
        self.month_end = month_end
        params = {
            "tenantId": self.tenantid,
            "fromDate": int(month_start.timestamp() * 1000),
            "toDate": int(month_end.timestamp() * 1000),
        }
        super().__init__(endpoint, headers, request_info, user_request, params, response_key, **kwargs)

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        :this response has only one object, so return it
        """
        expenses = response.json()[self.response_key]
        expenses["tenantId"] = self.tenantid
        expenses["fromDate"] = self.month_start.strftime("%Y-%m-%d")
        expenses["toDate"] = self.month_end.strftime("%Y-%m-%d")
        combined_string = f"{self.tenantid}{expenses['fromDate']}{expenses['toDate']}"
        id_hash = hashlib.sha256(combined_string.encode())
        return [{"data": expenses, "id": id_hash.hexdigest()}]


class MgramsevaTenantExpenses(MgramsevaStream):
    """object for tenant payments"""

    def __init__(
        self, headers: dict, request_info: dict, user_request: dict, tenantid_list: list, fromdate: datetime, todate: datetime, **kwargs
    ):  # pylint: disable=super-init-not-called
        """
        specify endpoint for demands and call super
        1672531200000 = 2023-01-01 00:00
        1830297600000 = 2028-01-01 00:00
        """
        self.headers = headers
        self.request_info = request_info
        self.user_request = user_request
        self.tenantid_list = tenantid_list
        self.fromdate = fromdate
        self.todate = todate

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[StreamData]:
        """override"""

        for tenantid in self.tenantid_list:

            month_start = self.fromdate.replace(day=1)

            while month_start < self.todate:

                next_month_start = month_start + relativedelta(months=1) - timedelta(milliseconds=1)

                stream = MgramsevaTenantExpense(
                    "echallan-services/eChallan/v1/_expenseDashboard",
                    self.headers,
                    self.request_info,
                    self.user_request,
                    tenantid,
                    month_start,
                    next_month_start,
                    "ExpenseDashboard",
                )
                yield from stream.read_records(sync_mode, cursor_field, stream_slice, stream_state)

                month_start = next_month_start


class MgramsevaPayments(MgramsevaStream):
    """object for consumer payments"""

    def __init__(
        self, headers: dict, request_info: dict, user_request: dict, tenantid_list: list, consumer_codes: dict, **kwargs
    ):  # pylint: disable=super-init-not-called
        """specify endpoint for payments and call super"""
        self.headers = headers
        self.request_info = request_info
        self.user_request = user_request
        self.tenantid_list = tenantid_list
        self.consumer_codes = consumer_codes

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[StreamData]:
        """override"""

        for tenantid in self.tenantid_list:
            for consumer_code in self.consumer_codes[tenantid]:
                params = {"tenantId": tenantid, "businessService": "WS", "consumerCodes": consumer_code}
                # self.logger.info("requesting payments for %s", consumer_code)
                paymentstream = MgramsevaStream(
                    "collection-services/payments/WS/_search", self.headers, self.request_info, self.user_request, params, "Payments"
                )
                yield from paymentstream.read_records(sync_mode, cursor_field, stream_slice, stream_state)


class MgramsevaWaterConnections(MgramsevaStream):
    """object for water connections"""

    def __init__(
        self, headers: dict, request_info: dict, user_request: dict, tenantid_list: list, **kwargs
    ):  # pylint: disable=super-init-not-called
        """specify endpoint for water connections and call super"""
        self.headers = headers
        self.request_info = request_info
        self.user_request = user_request
        self.tenantid_list = tenantid_list

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[StreamData]:
        """override"""

        for tenantid in self.tenantid_list:
            params = {"tenantId": tenantid, "businessService": "WS"}
            wcstream = MgramsevaStream(
                "ws-services/wc/_search", self.headers, self.request_info, self.user_request, params, "WaterConnection"
            )
            yield from wcstream.read_records(sync_mode, cursor_field, stream_slice, stream_state)


# Source
class SourceMgramseva(AbstractSource):
    """Source for mGramSeva"""

    def __init__(self):
        """constructor"""
        self.headers = {}
        self.request_info = {}
        self.user_request = {}
        self.base_url = None
        self.config = {}
        self.setup_complete = False

    def setup(self, config: dict) -> None:
        """
        config contains
        - base_url
        - client_user
        - client_password
        - username
        - password
        """
        if self.setup_complete:
            return
        if "client_password" not in config or config["client_password"] is None:
            config["client_password"] = ""
        if config["client_password"] == "no-pass":
            config["client_password"] = ""
        client_user_password = f'{config["client_user"]}:{config["client_password"]}'
        apikey = base64.encodebytes(client_user_password.encode("ascii")).decode("utf-8").strip()
        self.headers = {"Authorization": "Basic " + apikey}

        base_url = config["base_url"]
        if base_url[-1] != "/":
            base_url += "/"
        self.base_url = base_url

        self.config = config
        self.setup_complete = True

    def get_auth_token(self) -> None:
        """performs the auth step to get the access token and the user info"""

        response = requests.post(
            self.base_url + "user/oauth/token",
            params={
                "username": self.config["username"],
                "password": self.config["password"],
                "scope": "read",
                "grant_type": "password",
                "tenantId": "br",
                "userType": "EMPLOYEE",
            },
            headers=self.headers,
            timeout=15,
        )

        response.raise_for_status()

        auth_response = response.json()
        self.user_request = auth_response["UserRequest"]
        self.request_info = {
            "action": "_search",
            "apiId": "mgramseva",
            "authToken": auth_response["access_token"],
            "userInfo": self.user_request,
        }

    def check_connection(self, logger: Logger, config) -> Tuple[bool, any]:
        """attempt to connect to the API with the provided credentials"""
        try:
            self.setup(config)
            self.get_auth_token()
        except requests.HTTPError as e:
            logger.exception(e)
            return False, str(e)
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """return all the streams we have to sync"""

        self.setup(config)
        self.get_auth_token()

        # tenant_expenses_from = datetime.strptime(config.get("tenant_expenses_from", "2022-01-01"), "%Y-%m-%d")
        # tenant_expenses_to = datetime.strptime(config.get("tenant_expenses_to", "2022-01-01"), "%Y-%m-%d")

        start_date = datetime.strptime(config.get("start_date", "2022-01-01"), "%Y-%m-%d")
        start_date = pytz.IST.localize(start_date).astimezone(pytz.utc)
        end_date = datetime.today()
        end_date = pytz.IST.localize(end_date).astimezone(pytz.utc)

        # Generate streams for each object type
        streams = [
            MgramsevaWaterConnections(self.headers, self.request_info, self.user_request, self.config["tenantids"]),
            MgramsevaTenantExpenses(self.headers, self.request_info, self.user_request, self.config["tenantids"], start_date, end_date),
            MgramsevaDemands(self.headers, self.request_info, self.user_request, self.config["tenantids"]),
        ]

        # bills and payments require a list of consumer codes for each tenant
        tenantid_to_consumer_codes = {}
        for tenantid in self.config["tenantids"]:
            tenantid_to_consumer_codes[tenantid] = set()
            tmp_demand_stream = MgramsevaDemands(self.headers, self.request_info, self.user_request, [tenantid])
            for demand in tmp_demand_stream.read_records(SyncMode.full_refresh):
                tenantid_to_consumer_codes[tenantid].add(demand["data"]["consumerCode"])

        streams.append(
            MgramsevaBills(self.headers, self.request_info, self.user_request, self.config["tenantids"], tenantid_to_consumer_codes)
        )
        streams.append(
            MgramsevaPayments(self.headers, self.request_info, self.user_request, self.config["tenantids"], tenantid_to_consumer_codes)
        )

        return streams
