import pandas as pd
from etl_modules import transform as my_module
from unittest.mock import patch


@patch('etl_modules.transform.transform_weather')
def test_json_from_apis(mock_get):
    mock_get.return_value = pd.json_normalize({'coord': {'lon': -58.3772, 'lat': -34.6132},
                                               'weather': [{'id': 800, 'main': 'Clear', 'description': 'clear sky', 'icon': '01d'}],
                                               'base': 'stations',
                                               'main':
                                               {'temp': 19.73, 'feels_like': 19.51, 'temp_min': 18.85, 'temp_max': 21.66, 'pressure': 1018, 'humidity': 67},
                                               'visibility': 10000, 'wind': {'speed': 3.09, 'deg': 10}, 'clouds': {'all': 0}, 'dt': 1699273803,
                                               'sys': {'type': 1, 'id': 8224, 'country': 'AR', 'sunrise': 1699260443, 'sunset': 1699309612},
                                               'timezone': -10800, 'id': 3435910, 'name': 'Buenos Aires', 'cod': 200})

    data = my_module.transform_weather("/tmp/test.example.json")
    columns_data = data.columns
    excepted_output = 'dt'
    assert excepted_output in columns_data, f"expected value:{excepted_output} differs from actual value {columns_data}"
