CREATE TABLE IF NOT EXISTS eco_bikes_dataset.dim_date (
  date_id INT64 NOT NULL,
  date TIMESTAMP,
  week_day INT64,
  day_name STRING,
  day INT64,
  month INT64,
  month_name STRING,
  week INT64,
  quarter INT64,
  year INT64,
  is_month_start BOOL,
  is_month_end BOOL,
  reload_id INT64 NOT NULL,
);

CREATE TABLE IF NOT EXISTS eco_bikes_dataset.metadata_load (
  reload_id INT64 NOT NULL,
  date_reload TIMESTAMP,
  date_id INT64 NOT NULL,
);

CREATE TABLE IF NOT EXISTS eco_bikes_dataset.station_info_eco_bikes (
  pk_surrogate_station_info INT64,
  station_id STRING,
  station_name STRING,
  physical_configuration STRING,
  lat FLOAT64,
  lon FLOAT64,
  altitude FLOAT64,
  address STRING,
  capacity INT64,
  is_charging_station BOOL,
  nearby_distance FLOAT64,
  _ride_code_support BOOL,
  start_date TIMESTAMP NOT NULL,
  end_date TIMESTAMP DEFAULT TIMESTAMP '2261-12-30 00:00:00 UTC',
  is_active INT64 DEFAULT 1 NOT NULL
);

CREATE TABLE IF NOT EXISTS eco_bikes_dataset.temp_station_info (
  pk_surrogate_station_info INT64,
  station_id STRING,
  station_name STRING,
  physical_configuration STRING,
  lat FLOAT64,
  lon FLOAT64,
  altitude FLOAT64,
  address STRING,
  capacity INT64,
  is_charging_station BOOL,
  nearby_distance FLOAT64,
  _ride_code_support BOOL,
  start_date TIMESTAMP NOT NULL,
  end_date TIMESTAMP DEFAULT TIMESTAMP '2261-12-30 00:00:00 UTC',
  is_active INT64 DEFAULT 1 NOT NULL
);

CREATE TABLE IF NOT EXISTS eco_bikes_dataset.station_status_eco_bikes (
  station_id STRING NOT NULL,
  num_bikes_available INT64,
  num_bikes_disabled INT64,
  num_docks_available INT64,
  num_docks_disabled INT64,
  last_reported TIMESTAMP,
  status STRING,
  is_installed INT64,
  is_renting INT64,
  is_returning INT64,
  reload_id INT64 NOT NULL,
  pk_surrogate_station_info INT64 NOT NULL
);

CREATE TABLE IF NOT EXISTS eco_bikes_dataset.system_info_eco_bikes (
  last_updated TIMESTAMP,
  ttl INT64,
  system_id STRING,
  language STRING,
  name STRING,
  timezone STRING,
  build_version STRING,
  build_label STRING,
  build_hash STRING,
  build_number STRING,
  mobile_head_version STRING,
  mobile_minimum_supported_version STRING,
  vehicle_count_mechanical_count INT64,
  vehicle_count_ebike_count INT64,
  station_count INT64,
  reload_id INT64 NOT NULL
);

CREATE TABLE IF NOT EXISTS eco_bikes_dataset.weather (
  date_id INT64,
  id INT64,
  main STRING,
  description STRING,
  icon STRING,
  temp FLOAT64,
  feels_like FLOAT64,
  temp_min FLOAT64,
  temp_max FLOAT64,
  pressure INT64,
  humidity INT64,
  sea_level INT64,
  grnd_level INT64,
  dt TIMESTAMP,
  reload_id INT64 NOT NULL
);