CREATE TABLE IF NOT EXISTS eco_bikes.df_dim_date (
	date_id int8 NULL,
	"Date" timestamp NULL,
	week_day int4 NULL,
	day_name text NULL,
	"day" int4 NULL,
	"month" int4 NULL,
	month_name text NULL,
	week int8 NULL,
	quarter int4 NULL,
	"year" int4 NULL,
	is_month_start bool NULL,
	is_month_end bool NULL
);

CREATE TABLE IF NOT EXISTS eco_bikes.station_info_eco_bikes (
	station_id text NULL,
	station_name text NULL,
	physical_configuration text NULL,
	lat float8 NULL,
	lon float8 NULL,
	altitude float8 NULL,
	address text NULL,
	post_code text NULL,
	capacity int8 NULL,
	is_charging_station bool NULL,
	nearby_distance float8 NULL,
	"_ride_code_support" bool NULL,
	cross_street text NULL
);

CREATE TABLE IF NOT EXISTS eco_bikes.station_status_eco_bikes (
	station_id text NULL,
	num_bikes_available int8 NULL,
	num_bikes_disabled int8 NULL,
	num_docks_available int8 NULL,
	num_docks_disabled int8 NULL,
	last_reported timestamp NULL,
	status text NULL,
	is_installed int8 NULL,
	is_renting int8 NULL,
	is_returning int8 NULL
);

CREATE TABLE IF NOT EXISTS eco_bikes.system_info_eco_bikes (
	last_updated timestamp NULL,
	ttl int8 NULL,
	"data.system_id" text NULL,
	"data.language" text NULL,
	"data.name" text NULL,
	"data.timezone" text NULL,
	"data.build_version" text NULL,
	"data.build_label" text NULL,
	"data.build_hash" text NULL,
	"data.build_number" text NULL,
	"data.mobile_head_version" text NULL,
	"data.mobile_minimum_supported_version" text NULL,
	"data._vehicle_count._mechanical_count" int8 NULL,
	"data._vehicle_count._ebike_count" int8 NULL,
	"data._station_count" int8 NULL
);


CREATE TABLE IF NOT EXISTS eco_bikes.weather (
	date_id int8 NULL,
	id int8 NULL,
	main text NULL,
	description text NULL,
	icon text NULL,
	"temp" float8 NULL,
	feels_like float8 NULL,
	temp_min float8 NULL,
	temp_max float8 NULL,
	pressure int8 NULL,
	humidity int8 NULL,
	sea_level int8 NULL,
	grnd_level int8 NULL,
	dt timestamp NULL
);