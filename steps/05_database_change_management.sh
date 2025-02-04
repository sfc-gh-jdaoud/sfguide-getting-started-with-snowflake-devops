: '
-----------------------------------------------------------------
Perform the following changes after switching to the "dev" branch
-----------------------------------------------------------------

-- Add this view definition at the placeholder near the end of steps/03_harmonize_data.sql
CREATE OR REPLACE VIEW silver.attractions AS
SELECT
    city.geo_id,
    city.geo_name,
    count(case when category_main = 'Aquarium' THEN 1 END) aquarium_cnt,
    count(case when category_main = 'Zoo' THEN 1 END) zoo_cnt,
    count(case when category_main = 'Korean Restaurant' THEN 1 END) korean_restaurant_cnt
FROM us_addresses__poi.cybersyn.point_of_interest_index poi
JOIN us_addresses__poi.cybersyn.point_of_interest_addresses_relationships poi_add 
    ON poi_add.poi_id = poi.poi_id
JOIN us_addresses__poi.cybersyn.us_addresses address 
    ON address.address_id = poi_add.address_id
JOIN silver.major_us_cities city ON city.geo_id = address.id_city
WHERE true
    AND category_main in ('Aquarium', 'Zoo', 'Korean Restaurant')
    AND id_country = 'country/USA'
GROUP BY city.geo_id, city.geo_name;


-- Append the following column definitions to the column list of the CREATE OR ALTER TABLE in steps/04_orchestrate_jobs.sql
, aquarium_cnt int
, zoo_cnt int
, korean_restaurant_cnt int


-- Add the "attractions" view to the JOIN clause of the "vacation_spots_update" task in steps/04_orchestrate_jobs.sql
join silver.attractions att on att.geo_name = city.geo_name


-- Append the following updates to the WHEN MATCHED clause of the "vacation_spots_update" task in steps/04_orchestrate_jobs.sql
, vacation_spots.aquarium_cnt = harmonized_vacation_spots.aquarium_cnt
, vacation_spots.zoo_cnt = harmonized_vacation_spots.zoo_cnt
, vacation_spots.korean_restaurant_cnt = harmonized_vacation_spots.korean_restaurant_cnt


-- Append the following columns to the WHEN NOT MATCHED clause of the "vacation_spots_update" task in steps/04_orchestrate_jobs.sql
, harmonized_vacation_spots.aquarium_cnt
, harmonized_vacation_spots.zoo_cnt
, harmonized_vacation_spots.korean_restaurant_cnt


-- Add the following filter conditions to the WHERE clause of the "email_notification" task in steps/04_orchestrate_jobs.sql
and korean_restaurant_cnt > 0
and (zoo_cnt > 0 or aquarium_cnt > 0)


---------------------------------------------------------
Commit your changes to the "dev" branch before continuing
---------------------------------------------------------
'

# Fetch changes from GitHub
snow git fetch quickstart_common.public.quickstart_repo
# Deploy the updated data pipeline
snow git execute @quickstart_common.public.quickstart_repo/branches/dev/steps/0[134]_*
