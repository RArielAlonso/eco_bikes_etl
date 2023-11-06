from utils import utils as my_module
from unittest.mock import patch
from config.config import weather_ds


@patch('utils.utils.get_request_json')
def test_json_from_apis(mock_get):
    mock_get.return_value = {'coord': 10}
    data = my_module.get_request_json(weather_ds['base_url'], weather_ds['params'])
    excepted_output = {'coord': 10}
    assert excepted_output == data, f"expected value:{excepted_output} differs from actual value {data}"
