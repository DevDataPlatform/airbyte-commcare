#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

from abc import ABC
from datetime import datetime
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from urllib.parse import parse_qs

import requests
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import CheckpointMixin, Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator


def ensure_single_trailing_Z(dtstr: str):
    """return the dtstr with a trailing Z, appending one if it's missing"""
    if dtstr.endswith("Z"):
        return dtstr
    return dtstr + "Z"


def parse_datetime_with_microseconds(dtstr: str):
    """parse a datetime string with or without microseconds"""
    for date_format in ["%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%f"]:
        try:
            return datetime.strptime(dtstr, date_format)
        except ValueError:
            pass
    raise ValueError(f"Could not parse datetime string {dtstr}")


# Basic full refresh stream
class CommcareStream(HttpStream, ABC):
    def __init__(self, project_space, form_fields_to_exclude, **kwargs):
        super().__init__(**kwargs)
        self.project_space = project_space
        self.form_fields_to_exclude = form_fields_to_exclude

    @property
    def url_base(self) -> str:
        return f"https://www.commcarehq.org/a/{self.project_space}/api/v0.5/"

    # These class variables save state
    # forms holds form ids and we filter cases which contain one of these form ids
    # last_form_date stores the date of the last form read so the next cycle for forms and cases starts at the same timestamp
    forms = set()
    last_form_date = None
    schemas = {}

    @property
    def dateformat_for_query(self) -> str:
        return "%Y-%m-%dT%H:%M:%S.%f"

    def scrubUnwantedFields(self, form: dict[str, str]) -> dict:
        new_dict = {}
        for key, value in form.items():
            if key in self.form_fields_to_exclude:
                continue
            if any(key.startswith(prefix) for prefix in self.form_fields_to_exclude):
                continue
            if isinstance(value, dict):
                new_dict[key] = self.scrubUnwantedFields(value)
            else:
                new_dict[key] = value
        return new_dict

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        try:
            # Server returns status 500 when there are no more rows.
            # raise an error if server returns an error
            response.raise_for_status()
            meta = response.json()["meta"]
            return parse_qs(meta["next"][1:])
        except Exception as ex:
            return ex

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = {"format": "json"}
        return params


class Application(CommcareStream):
    primary_key = "id"

    def __init__(self, app_id, **kwargs):
        super().__init__(**kwargs)
        self.app_id = app_id

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return f"application/{self.app_id}/"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = {"format": "json", "extras": "true"}
        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        yield response.json()


class IncrementalStream(CommcareStream, CheckpointMixin):
    cursor_field = "indexed_on"
    _cursor_value = None

    @property
    def state(self) -> Mapping[str, Any]:
        return {self.cursor_field: self._cursor_value}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        if self.cursor_field in value:
            self._cursor_value = parse_datetime_with_microseconds(value[self.cursor_field])

    @property
    def sync_mode(self):
        return SyncMode.incremental

    @property
    def supported_sync_modes(self):
        return [SyncMode.incremental]

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        try:
            # Server returns status 500 when there are no more rows.
            # raise an error if server returns an error
            response.raise_for_status()
            meta = response.json()["meta"]
            if meta["next"]:
                return parse_qs(meta["next"][1:])
            return None
        except Exception:
            return None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = {"format": "json"}
        if next_page_token:
            params.update(next_page_token)
        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        for o in iter(response.json()["objects"]):
            yield o
        return None


class Case(IncrementalStream):
    """
    docs: https://www.commcarehq.org/a/[domain]/api/[version]/case/
    """

    cursor_field = "indexed_on"
    primary_key = "id"

    def __init__(self, start_date, schema, app_id, **kwargs):
        super().__init__(**kwargs)
        self._cursor_value = parse_datetime_with_microseconds(start_date)
        self.schema = schema
        self.last_record = None

    def get_json_schema(self):
        return self.schema

    @property
    def name(self):
        # Airbyte orders streams in alpha order but since we have dependent peers and we need to
        # pull all forms before cases, we name this stream to
        # ensure this stream gets pulled last (assuming ascii stream names only)
        return "zzz_case"

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "case"

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        # start date is what we saved for forms
        # if self.cursor_field in self.state else (CommcareStream.last_form_date or self.initial_date)
        ix: datetime = self.state[self.cursor_field]
        LIMIT = 5000
        params = {
            "format": "json",
            "indexed_on_start": ix.strftime(self.dateformat_for_query),
            "order_by": "indexed_on",
            "limit": str(LIMIT),
        }
        if next_page_token:
            # next_page_token = {'format': ['json'], 'indexed_on_start': ['2024-12-01T00:00:00.000000'], 'order_by': ['indexed_on'], 'limit': ['5000'], 'offset': ['5000']}
            MAX_OFFSET = 900000
            if "offset" in next_page_token and int(next_page_token["offset"][0]) >= MAX_OFFSET:
                self.logger.info("=========== last_record, params before, params after ===========")
                self.logger.info(self.last_record)
                self.logger.info(params)
                params.update(next_page_token)
                params["indexed_on_start"] = self.last_record[self.cursor_field].replace("Z", "")
                params["offset"] = "0"
                self.logger.info(params)
                self.logger.info("================================================================")
            else:
                params.update(next_page_token)
        return params

    def read_records(self, *args, **kwargs) -> Iterable[Mapping[str, Any]]:
        for record in super().read_records(*args, **kwargs):
            self.last_record = record
            if any(f in CommcareStream.forms for f in record["xform_ids"]):
                self._cursor_value = parse_datetime_with_microseconds(record[self.cursor_field])
                # Make indexed_on tz aware
                record.update({"streamname": "case", "indexed_on": ensure_single_trailing_Z(record["indexed_on"])})
                # convert xform_ids field from array to comma separated list so flattening won't create
                # one field per item. This is because some cases have up to 2000 xform_ids and we don't want 2000 extra
                # fields in the schema
                record["xform_ids"] = ",".join(record["xform_ids"])
                retval = {}
                retval["id"] = record["id"]
                retval["indexed_on"] = ensure_single_trailing_Z(record["indexed_on"])
                retval["data"] = record
                yield retval
        if self._cursor_value.microsecond == 0:
            # Airbyte converts the cursor_field value (datetime) to string when it saves the state and
            # our state setter parses the saved state with a format that contains microseconds
            # self._cursor_value must have non-zero microseconds for the formatting and parsing to work correctly.
            # This issue would also occur if an incoming record had a timestamp with zero microseconds
            self._cursor_value = self._cursor_value.replace(microsecond=10)
        # This cycle of pull is complete so clear out the form ids we saved for this cycle
        CommcareStream.forms.clear()


class Form(IncrementalStream):
    """
    docs: https://www.commcarehq.org/a/[domain]/api/[version]/form/
    """

    cursor_field = "indexed_on"
    primary_key = "id"

    def __init__(self, start_date, app_id, name, xmlns, schema, **kwargs):
        super().__init__(**kwargs)
        self.app_id = app_id
        self._cursor_value = parse_datetime_with_microseconds(start_date)
        self.streamname = name
        self.xmlns = xmlns
        self.schema = schema

    @property
    def name(self):
        return getattr(self, 'streamname', 'form')

    def get_json_schema(self):
        return self.schema

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "form"

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        # if self.cursor_field in self.state else self.initial_date
        ix: datetime = self.state[self.cursor_field]
        params = {
            "format": "json",
            "app_id": self.app_id,
            "indexed_on_start": ix.strftime(self.dateformat_for_query),
            "order_by": "indexed_on",
            "limit": "1000",
            "xmlns": self.xmlns,
        }
        if next_page_token:
            params.update(next_page_token)
        return params

    def read_records(self, *args, **kwargs) -> Iterable[Mapping[str, Any]]:
        for record in super().read_records(*args, **kwargs):
            self._cursor_value = parse_datetime_with_microseconds(record[self.cursor_field])
            CommcareStream.forms.add(record["id"])
            newform = self.scrubUnwantedFields(record)
            retval = {}
            retval["id"] = newform["id"]
            newform[self.cursor_field] = ensure_single_trailing_Z(newform[self.cursor_field])
            retval[self.cursor_field] = newform[self.cursor_field]
            retval["data"] = newform
            yield retval
        if self._cursor_value.microsecond == 0:
            # Airbyte converts the cursor_field value (datetime) to string when it saves the state and
            # our state setter parses the saved state with a format that contains microseconds
            # self._cursor_value must have non-zero microseconds for the formatting and parsing to work correctly.
            # This issue would also occur if an incoming record had a timestamp with zero microseconds
            self._cursor_value = self._cursor_value.replace(microsecond=10)


# Source
class SourceCommcare(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            auth = TokenAuthenticator(config["api_key"], auth_method="ApiKey")
            args = {
                "authenticator": auth,
            }
            form_fields_to_exclude = config.get("form_fields_to_exclude", [])
            next(
                Application(
                    **{
                        **args,
                        "app_id": config["app_id"],
                        "form_fields_to_exclude": form_fields_to_exclude,
                        "project_space": config["project_space"],
                    }
                )
                .read_records(SyncMode.full_refresh)
            )
            return True, None
        except Exception as error:
            return False, " Invalid apikey, project_space or app_id : " + str(error)

    def base_schema(self):
        return {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "id": {"type": "string"},
                "indexed_on": {"type": "string", "format": "date-time", "airbyte_type": "timestamp_with_timezone"},
                "data": {"type": "object"},
            },
        }

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = TokenAuthenticator(config["api_key"], auth_method="ApiKey")
        args = {
            "authenticator": auth,
        }
        form_fields_to_exclude = config.get("form_fields_to_exclude", [])
        appdata = Application(
            **{
                **args,
                "app_id": config["app_id"],
                "form_fields_to_exclude": form_fields_to_exclude,
                "project_space": config["project_space"],
            }
        ).read_records(sync_mode=SyncMode.full_refresh)

        # Generate streams for forms, one per xmlns and one stream for cases.
        streams = self.generate_streams(args, config, appdata)
        return streams

    def generate_streams(self, args, config, appdata):
        form_fields_to_exclude = config.get("form_fields_to_exclude", [])
        form_args = {
            "app_id": config["app_id"],
            "start_date": config["start_date"],
            "form_fields_to_exclude": form_fields_to_exclude,
            "project_space": config["project_space"],
            **args,
        }
        streams = []
        name2xmlns = {}

        # Collect the form names and xmlns from the application
        for record in appdata:
            mods = record["modules"]
            for m in mods:
                forms = m["forms"]
                for f in forms:
                    xmlns = f["xmlns"]
                    formname = ""
                    if "en" in f["name"]:
                        formname = f["name"]["en"].strip()
                    else:
                        # Unknown forms are named UNNAMED_xxxxx where xxxxx are the last 5 digits of the XMLNS
                        # This convention gives us repeatable names
                        formname = f"Unnamed_{xmlns[-5:]}"

                    name = formname
                    name2xmlns[name] = xmlns

        # Create the streams from the collected names
        # Sorted by name
        for k in sorted(name2xmlns):
            key = name2xmlns[k]
            stream = Form(name=k, xmlns=key, schema=self.base_schema(), **form_args)
            streams.append(stream)

        stream = Case(
            app_id=config["app_id"],
            start_date=config["start_date"],
            schema=self.base_schema(),
            project_space=config["project_space"],
            form_fields_to_exclude=form_fields_to_exclude,
            **args,
        )

        streams.append(stream)

        return streams
