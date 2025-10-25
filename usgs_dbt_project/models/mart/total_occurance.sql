SELECT count(*) AS Total_events
FROM {{ ref('stg_usgs_events') }}