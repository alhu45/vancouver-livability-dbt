{{ config(
    materialized = 'view',
    schema = 'SILVER'
) }}

SELECT *
FROM VANCOUVER_DATA.SILVER.TRANSIT_STOPS_CLEAN