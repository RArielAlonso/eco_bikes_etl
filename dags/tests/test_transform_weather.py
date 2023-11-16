from unittest.mock import patch

from etl_modules.transform import transform_weather


@patch("utils.utils.json.load")
@patch("utils.utils.open")
def test_transform_weather(mock_open, mock_json_load):
    mock_json_load.return_value = dict(
        {
            "coord": {"lon": -58.3772, "lat": -34.6132},
            "weather": [
                {"id": 800, "main": "Clear", "description": "clear sky", "icon": "01d"}
            ],
            "base": "stations",
            "main": {
                "temp": 19.73,
                "feels_like": 19.51,
                "temp_min": 18.85,
                "temp_max": 21.66,
                "pressure": 1018,
                "humidity": 67,
            },
            "visibility": 10000,
            "wind": {"speed": 3.09, "deg": 10},
            "clouds": {"all": 0},
            "dt": 1699273803,
            "sys": {
                "type": 1,
                "id": 8224,
                "country": "AR",
                "sunrise": 1699260443,
                "sunset": 1699309612,
            },
            "timezone": -10800,
            "id": 3435910,
            "name": "Buenos Aires",
            "cod": 200,
        }
    )
    data = transform_weather(mock_json_load)
    columns_data = data.columns
    column_check = "dt"
    assert column_check in columns_data
