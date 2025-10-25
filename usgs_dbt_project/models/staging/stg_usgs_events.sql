{{ config(
    materialized='view',
    unique_key=['id', 'event_time'],
    on_schema_change='sys',
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
        lat,
        lon,
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

        -- depth depth category
        CASE 
            WHEN depth_km BETWEEN -10 AND 70  THEN 'Shallow'
            WHEN depth_km BETWEEN 70 AND 300 THEN 'Intermediate'
            ELSE 'Deep'
        END AS dep_category,

        -- Create a simplified region 
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

SELECT * FROM transformed
WHERE 
    mag IS NOT NULL
    AND lon IS NOT NULL
    AND lat IS NOT NULL
    AND depth_km IS NOT NULL
    AND event_time IS NOT NULL
    AND region IS NOT NULL

{% if is_incremental() %}
  WHERE event_time > (SELECT MAX(event_time) FROM {{ this }})
{% endif %}