from utils import utils as my_module
from unittest.mock import patch


@patch('utils.utils.get_request_json')
def test_json_from_apis(mock_get):
    mock_get.return_value = {'coord': 10}
    data = my_module.get_request_json()
    excepted_output = {'coord': 10}
    assert excepted_output == data, f"expected value:{excepted_output} differs from actual value {data}"
