from etl_modules.extract import extract as my_module
from unittest.mock import patch
from config.config import extract_list


@patch('etl_modules.extract.extract')
def test_json_paths_from_apis(mock_get):
    mock_get.return_value.json.return_value = {'weather': '/tmp/weather.json',
                                               'system_info_eco_bikes': '/tmp/system_info_eco_bikes.json',
                                               'station_status_eco_bikes': '/tmp/station_status_eco_bikes.json',
                                               'station_info_eco_bikes': '/tmp/station_info_eco_bikes.json'}
    excepted_output = {'weather': '/tmp/weather.json',
                       'system_info_eco_bikes': '/tmp/system_info_eco_bikes.json',
                       'station_status_eco_bikes': '/tmp/station_status_eco_bikes.json',
                       'station_info_eco_bikes': '/tmp/station_info_eco_bikes.json'}
    actual_output = my_module(extract_list)

    assert excepted_output == actual_output, f"expected value:{excepted_output} differs from actual value {actual_output}"
