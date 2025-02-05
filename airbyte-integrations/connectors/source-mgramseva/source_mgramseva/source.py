#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
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

    def __init__(self, endpoint: str, headers: dict, request_info: dict, user_request: dict, response_key: str, **kwargs):
        """set base url, headers, request info and user request"""
        super().__init__(**kwargs)
        self.endpoint = endpoint
        self.headers = headers
        self.request_info = request_info
        self.user_request = user_request
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

    def get_next_params(self) -> Optional[Mapping[str, Any]]:
        """Returns the next available parameters (for pagination)"""
        raise NotImplementedError

    def request_params(self, stream_state, stream_slice=None, next_page_token=None):
        """Returns the request parameters for the API call."""
        # We need params in first api call also and next_page_token will not be called for first call
        if next_page_token is None:
            next_page_token = self.get_next_params() 
        return next_page_token or {}

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """Determines the next page token for pagination."""
        return self.get_next_params()

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
        endpoint = "billing-service/demand/_search"
        response_key = "Demands"
        super().__init__(endpoint, headers, request_info, user_request, response_key, **kwargs)
        self.tenant_index = 0  
        self.tenantid_list = tenantid_list

    def get_next_params(self) -> Optional[Mapping[str, Any]]:
        """Returns the next available parameters (used for both first and subsequent requests)."""

        if self.tenant_index < len(self.tenantid_list):  
            tenantid = self.tenantid_list[self.tenant_index]
            next_params = {"tenantId": tenantid, "businessService": "WS"}
            self.tenant_index += 1
            return next_params
        return None
    
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
        endpoint = "billing-service/bill/v2/_fetchbill"  
        response_key = "Bill"  
        super().__init__(endpoint, headers, request_info, user_request, response_key, **kwargs)
        self.tenant_index = 0  
        self.consumer_index = 0  
        self.tenantid_list = tenantid_list
        self.consumer_codes = consumer_codes

    def get_next_params(self) -> Optional[Mapping[str, Any]]:
        """Returns the next available parameters (used for both first and subsequent requests)."""
        while self.tenant_index < len(self.tenantid_list):  
            tenantid = self.tenantid_list[self.tenant_index]
            consumer_list = self.consumer_codes.get(tenantid, [])
            if self.consumer_index < len(consumer_list):
                consumer_code = consumer_list[self.consumer_index]
                self.consumer_index += 1
                next_params = {"tenantId": tenantid, "businessService": "WS", "consumerCode": consumer_code}
                return next_params
            self.consumer_index = 0
            self.tenant_index += 1  
        return None 


class MgramsevaTenantExpenses(MgramsevaStream):
    """Object for tenant expenses"""

    def __init__(
        self, headers: dict, request_info: dict, user_request: dict, tenantid_list: list, fromdate: datetime, todate: datetime, **kwargs
    ):  
        """Initialize the stream with parameters"""
        endpoint = "echallan-services/eChallan/v1/_expenseDashboard"
        response_key = "ExpenseDashboard"
        super().__init__(endpoint, headers, request_info, user_request, response_key, **kwargs)
        
        # Initialize instance variables
        self.tenantid_list = tenantid_list
        self.fromdate = fromdate.replace(day=1)  # Start from the first day of the month
        self.todate = todate
        self.tenant_index = 0
        self.current_month = self.fromdate
        
        # Variables to track tenant and date range
        self.curr_tenant_id = None
        self.curr_tenant_start_month = None
        self.curr_tenant_end_month = None

    def get_next_params(self) -> Optional[Mapping[str, Any]]:
        """Returns the next available parameters for the request"""
        while self.tenant_index < len(self.tenantid_list):
            tenantid = self.tenantid_list[self.tenant_index]
            
            if self.current_month < self.todate:

                next_month_start = self.current_month + relativedelta(months=1) - timedelta(milliseconds=1)

                self.curr_tenant_id = tenantid
                self.curr_tenant_start_month = self.current_month
                self.curr_tenant_end_month = next_month_start

                params = {
                    "tenantId": tenantid,
                    "fromDate": int(self.current_month.timestamp() * 1000),
                    "toDate": int(next_month_start.timestamp() * 1000),
                }
                
                # Move to the next month
                self.current_month = next_month_start + timedelta(milliseconds=1)

                # If we've processed all months for a tenant, move to the next tenant
                if self.current_month >= self.todate:
                    self.current_month = self.fromdate  # Reset for next tenant
                    self.tenant_index += 1
                
                return params
            
            self.tenant_index += 1  # Move to next tenant if months are exhausted
        
        return None  

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        :this response has only one object, so return it
        """
        expenses = response.json()[self.response_key]
        expenses["tenantId"] = self.curr_tenant_id
        expenses["fromDate"] = self.curr_tenant_start_month.strftime("%Y-%m-%d")
        expenses["toDate"] = self.curr_tenant_end_month.strftime("%Y-%m-%d")
        combined_string = f"{self.curr_tenant_id}{expenses['fromDate']}{expenses['toDate']}"
        id_hash = hashlib.sha256(combined_string.encode())
        return [{"data": expenses, "id": id_hash.hexdigest()}]


class MgramsevaPayments(MgramsevaStream):
    """object for consumer payments"""

    def __init__(
        self, headers: dict, request_info: dict, user_request: dict, tenantid_list: list, consumer_codes: dict, **kwargs
    ):  # pylint: disable=super-init-not-called

        endpoint = "collection-services/payments/WS/_search"  
        response_key = "Payments"  
        super().__init__(endpoint, headers, request_info, user_request, response_key, **kwargs)
        self.tenant_index = 0  
        self.consumer_index = 0 
        self.tenantid_list = tenantid_list
        self.consumer_codes = consumer_codes
        
    def get_next_params(self) -> Optional[Mapping[str, Any]]:
        """Returns the next available parameters (used for both first and subsequent requests)."""
        while self.tenant_index < len(self.tenantid_list):  
            tenantid = self.tenantid_list[self.tenant_index]
            consumer_list = self.consumer_codes.get(tenantid, [])

            if self.consumer_index < len(consumer_list):
                consumer_code = consumer_list[self.consumer_index]
                self.consumer_index += 1
                next_params = {"tenantId": tenantid, "businessService": "WS", "consumerCodes": consumer_code}
                return next_params

            self.consumer_index = 0
            self.tenant_index += 1  

        return None 


class MgramsevaWaterConnections(MgramsevaStream):
    """object for water connections"""

    def __init__(
        self, headers: dict, request_info: dict, user_request: dict, tenantid_list: list, **kwargs
    ):  # pylint: disable=super-init-not-called
        endpoint = "ws-services/wc/_search"
        response_key = "WaterConnection"
        super().__init__(endpoint, headers, request_info, user_request, response_key, **kwargs)
        self.tenant_index = 0  
        self.tenantid_list = tenantid_list

    def get_next_params(self) -> Optional[Mapping[str, Any]]:
        """Returns the next available parameters (used for both first and subsequent requests)."""
        if self.tenant_index < len(self.tenantid_list):  
            tenantid = self.tenantid_list[self.tenant_index]
            next_params = {"tenantId": tenantid, "businessService": "WS"}
            self.tenant_index += 1
            return next_params
        return None


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
            tenantid_to_consumer_codes[tenantid] = list(tenantid_to_consumer_codes[tenantid])

        streams.append(
            MgramsevaBills(self.headers, self.request_info, self.user_request, self.config["tenantids"], tenantid_to_consumer_codes)
        )
        streams.append(
            MgramsevaPayments(self.headers, self.request_info, self.user_request, self.config["tenantids"], tenantid_to_consumer_codes)
        )

        return streams
    