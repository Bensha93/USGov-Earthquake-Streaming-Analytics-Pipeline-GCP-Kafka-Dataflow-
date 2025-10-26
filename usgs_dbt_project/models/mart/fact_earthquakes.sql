{{ config(
    materialized='incremental',
    unique_key='id',
    partition_by={'field': 'event_date', 'data_type': 'date'},
    incremental_strategy='merge'
) }}

SELECT
    id,
    region,
    event_date,
    mag,
    mag_category,
    dep_category,
    tsunami,
    continent
FROM {{ ref('stg_usgs_events') }}


{% if is_incremental() %}
  WHERE mag IS NOT NULL AND event_date > (SELECT MAX(event_date) FROM {{ this }})
{% endif %}
