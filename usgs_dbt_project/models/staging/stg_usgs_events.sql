{{ config(
    materialized='view',
    unique_key=['id', 'event_time'],
    on_schema_change='sync',
    tags=['earthquake', 'usgs', 'usgs_events', 'seismology'],
    persist_docs={"relation": true, "columns": true},
    incremental_strategy='merge'
) }}

WITH source AS (
    SELECT
        id,
        TIMESTAMP_MILLIS(time) AS event_time,
        mag,
        place,
        url,
        CAST(lat AS FLOAT64) AS lat,
CAST(lon AS FLOAT64) AS lon,
        depth_km,
        felt,
        cdi,
        mmi,
        alert,
        tsunami,
        type
    FROM {{ source('earthquake_data', 'usgs_events') }}
),

transformed AS (
    SELECT
        *,
        -- Create magnitude category 
        CASE 
            WHEN mag BETWEEN -3 AND 2 THEN 'Minor'
            WHEN mag <= 4 THEN 'Light'
            WHEN mag <= 6 THEN 'Moderate'
            ELSE 'Major'
        END AS mag_category,

        -- depth category
        CASE 
            WHEN depth_km BETWEEN -10 AND 70  THEN 'Shallow'
            WHEN depth_km BETWEEN 70 AND 300 THEN 'Intermediate'
            ELSE 'Deep'
        END AS dep_category,

        -- Simplified region 
        CASE
            WHEN STRPOS(place, 'of ') > 0 THEN SPLIT(place, 'of ')[SAFE_OFFSET(1)]
            ELSE place
        END AS region,

        -- Boolean flag for tsunami alerts
        CASE
            WHEN tsunami = 0 THEN 'FALSE' 
            WHEN tsunami = 1 THEN 'TRUE'
        END AS is_tsunami,

        -- Derived date/time fields
        EXTRACT(DATE FROM event_time) AS event_date,
        EXTRACT(HOUR FROM event_time) AS event_hour
    FROM source
)

SELECT 
    t.*,
    c.continentLabel AS continent
FROM transformed t
LEFT JOIN {{ source('earthquake_data', 'continent_bounds') }} c
    ON t.lat BETWEEN c.minLat AND c.maxLat
   AND t.lon BETWEEN c.minLon AND c.maxLon
WHERE 
    t.mag IS NOT NULL
    AND t.lon IS NOT NULL
    AND t.lat IS NOT NULL
    AND t.depth_km IS NOT NULL
    AND t.event_time IS NOT NULL
    AND t.region IS NOT NULL

{% if is_incremental() %}
  AND t.event_time > (SELECT MAX(event_time) FROM {{ this }})
{% endif %}
