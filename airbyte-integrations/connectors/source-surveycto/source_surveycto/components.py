#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from typing import Any, List, Mapping

import requests

from airbyte_cdk.sources.declarative.extractors.record_extractor import RecordExtractor


class CustomExtractor(RecordExtractor):
    def extract_records(self, response: requests.Response, **kwargs) -> List[Mapping[str, Any]]:
        response_json = response.json()
        parsed_records = []

        for record in response_json:

            record_id = record.get("KEY")
            submission_date = record.get("SubmissionDate")
            endtime = record.get("endtime")

            parsed_record = {
                "KEY": record_id,
                "data": record,
                "SubmissionDate": submission_date,
                "endtime": endtime,
            }

            parsed_records.append(parsed_record)

        return parsed_records