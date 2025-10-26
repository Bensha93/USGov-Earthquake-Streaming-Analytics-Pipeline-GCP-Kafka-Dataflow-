{{ config(
    materialized='table',
    incremental_strategy='merge'
) }}

SELECT
    DISTINCT region,
    COUNT(id) AS total_events,
    AVG(mag) AS avg_mag,
    MAX(event_date) AS last_event_date
FROM {{ ref('stg_usgs_events') }}
GROUP BY region

{% if is_incremental() %}
  WHERE last_event_date > (SELECT MAX(last_event_date) FROM {{ this }})
{% endif %}