#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from unittest.mock import MagicMock, Mock, patch

import pytest
from source_commcare.source import SourceCommcare


@pytest.fixture(name="config")
def config_fixture():
    return {"api_key": "apikey", "app_id": "appid", "project_space": "project_space", "start_date": "2022-01-01T00:00:00Z",'form_fields_to_exclude':[]}


@patch("source_commcare.source.Application.read_records")
def test_check_connection_success(mock_read_records,config):
    mock_read_records.return_value = iter(["dummy_record"])

    source = SourceCommcare()
    logger_mock = Mock()

    result = source.check_connection(logger_mock, config=config)

    assert result == (True, None)
    mock_read_records.assert_called_once()

def test_check_connection_fail(mocker, config):
    source = SourceCommcare()
    logger_mock = MagicMock()
    excepted_outcome = " Invalid apikey, project_space or app_id : 'api_key'"
    assert source.check_connection(logger_mock, config={}) == (False, excepted_outcome)
