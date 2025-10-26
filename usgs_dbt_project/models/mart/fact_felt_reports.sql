{{ config(
    materialized='view',
    partition_by={'field': 'event_date', 'data_type': 'date'},
    incremental_strategy='merge'
) }}

SELECT
    event_date,
    region,
    AVG(cdi) AS avg_intensity,
    SUM(felt) AS total_reports
FROM {{ ref('stg_usgs_events') }}
WHERE felt IS NOT NULL
GROUP BY event_date, region
