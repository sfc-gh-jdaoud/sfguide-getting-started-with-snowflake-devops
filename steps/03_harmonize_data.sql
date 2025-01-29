-- Create UDF to map cities to airports
CREATE OR REPLACE FUNCTION silver.get_city_for_airport(iata VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$
from snowflake.snowpark.files import SnowflakeFile
from _snowflake import vectorized
import pandas
import json

@vectorized(input=pandas.DataFrame)
def main(df):
    airport_list = json.loads(
        SnowflakeFile.open("@bronze.raw/airport_list.json", 'r', require_scoped_url = False).read()
    )
    airports = {airport[3]: airport[1] for airport in airport_list}
    return df[0].apply(lambda iata: airports.get(iata.upper()))
$$;

-- Create view for flight emissions
CREATE OR REPLACE VIEW silver.flight_emissions AS
SELECT 
    departure_airport, 
    arrival_airport, 
    avg(estimated_co2_total_tonnes / seats) * 1000 as co2_emissions_kg_per_person
FROM oag_flight_emissions_data_sample.public.estimated_emissions_schedules_sample
WHERE seats != 0 AND estimated_co2_total_tonnes is not null
GROUP BY departure_airport, arrival_airport;

-- Create view for flight punctuality
CREATE OR REPLACE VIEW silver.flight_punctuality AS
SELECT 
    departure_iata_airport_code, 
    arrival_iata_airport_code, 
    count(
        case when arrival_actual_ingate_timeliness IN ('OnTime', 'Early') THEN 1 END
    ) / COUNT(*) * 100 as punctual_pct
FROM oag_flight_status_data_sample.public.flight_status_latest_sample
WHERE arrival_actual_ingate_timeliness is not null
GROUP BY departure_iata_airport_code, arrival_iata_airport_code;

-- Create view for flights from home
CREATE OR REPLACE VIEW silver.flights_from_home AS
SELECT 
    departure_airport, 
    arrival_airport, 
    get_city_for_airport(arrival_airport) arrival_city,  
    co2_emissions_kg_per_person, 
    punctual_pct
FROM silver.flight_emissions
JOIN silver.flight_punctuality 
    ON departure_airport = departure_iata_airport_code 
    AND arrival_airport = arrival_iata_airport_code
WHERE departure_airport = (
    SELECT $1:airport 
    FROM @quickstart_common.public.quickstart_repo/branches/main/data/home.json 
        (FILE_FORMAT => bronze.json_format));

-- Create view for weather forecast
CREATE OR REPLACE VIEW silver.weather_forecast AS
SELECT 
    postal_code, 
    avg(avg_temperature_air_2m_f) avg_temperature_air_f, 
    avg(avg_humidity_relative_2m_pct) avg_relative_humidity_pct, 
    avg(avg_cloud_cover_tot_pct) avg_cloud_cover_pct, 
    avg(probability_of_precipitation_pct) precipitation_probability_pct
FROM global_weather__climate_data_for_bi.standard_tile.forecast_day
WHERE country = 'US'
GROUP BY postal_code;

-- Create view for major US cities
CREATE OR REPLACE VIEW silver.major_us_cities AS
SELECT 
    geo.geo_id, 
    geo.geo_name, 
    max(ts.value) total_population
FROM global_government.cybersyn.datacommons_timeseries ts
JOIN global_government.cybersyn.geography_index geo 
    ON ts.geo_id = geo.geo_id
JOIN global_government.cybersyn.geography_relationships geo_rel 
    ON geo_rel.related_geo_id = geo.geo_id
WHERE true
    AND ts.variable_name = 'Total Population, census.gov'
    AND date >= '2020-01-01'
    AND geo.level = 'City'
    AND geo_rel.geo_id = 'country/USA'
    AND value > 100000
GROUP BY geo.geo_id, geo.geo_name
ORDER BY total_population DESC;

-- Create view for zip codes in city
CREATE OR REPLACE VIEW silver.zip_codes_in_city AS
SELECT 
    city.geo_id city_geo_id, 
    city.geo_name city_geo_name, 
    city.related_geo_id zip_geo_id, 
    city.related_geo_name zip_geo_name
FROM us_addresses__poi.cybersyn.geography_relationships country
JOIN us_addresses__poi.cybersyn.geography_relationships city 
    ON country.related_geo_id = city.geo_id
WHERE true
    AND country.geo_id = 'country/USA'
    AND city.level = 'City'
    AND city.related_level = 'CensusZipCodeTabulationArea'
ORDER BY city_geo_id;

-- Create view for weather joined with major cities
CREATE OR REPLACE VIEW silver.weather_joined_with_major_cities AS
SELECT 
    city.geo_id, 
    city.geo_name, 
    city.total_population,
    avg(avg_temperature_air_f) avg_temperature_air_f,
    avg(avg_relative_humidity_pct) avg_relative_humidity_pct,
    avg(avg_cloud_cover_pct) avg_cloud_cover_pct,
    avg(precipitation_probability_pct) precipitation_probability_pct
FROM silver.major_us_cities city
JOIN silver.zip_codes_in_city zip ON city.geo_id = zip.city_geo_id
JOIN silver.weather_forecast weather ON zip.zip_geo_name = weather.postal_code
GROUP BY city.geo_id, city.geo_name, city.total_population;
