{{ config(
    materialized='view',
    partition_by={'field': 'event_date', 'data_type': 'date'},
    incremental_strategy='merge'
) }}

SELECT
    region,
    event_date,
    COUNTIF(CAST(tsunami AS BOOL)) AS tsunami_event_count,
    AVG(mag) AS avg_mag_tsunami,
    MAX(mmi) AS max_intensity
FROM {{ ref('stg_usgs_events') }}
WHERE is_tsunami = 'TRUE'
GROUP BY region, event_date

{% if is_incremental() %}
  WHERE event_date > (SELECT MAX(event_date) FROM {{ this }})
{% endif %}