{{ config(
    materialized='incremental',
    unique_key='event_date',
    partition_by={'field': 'event_date', 'data_type': 'date'}
) }}

SELECT
    event_date,
    COUNT(id) AS total_events,
    AVG(mag) AS avg_mag,
    MAX(mag) AS max_mag,
    COUNTIF(CAST(is_tsunami AS BOOL)) AS tsunami_events
FROM {{ ref('stg_usgs_events') }}
GROUP BY event_date
