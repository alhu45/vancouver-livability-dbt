{{ config(
    materialized='table',
    schema='GOLD'
) }}

WITH base AS (
    SELECT
        NEIGHBOURHOOD,
        YEAR,
        CRIME_TYPE,
        COUNT(*) AS incident_count
    FROM {{ ref('stg_crime') }}
    GROUP BY NEIGHBOURHOOD, YEAR, CRIME_TYPE
),

with_window_functions AS (
    SELECT
        NEIGHBOURHOOD,
        YEAR,
        CRIME_TYPE,
        incident_count,

        -- Year-over-year change per neighbourhood
        LAG(incident_count) OVER (
            PARTITION BY NEIGHBOURHOOD, CRIME_TYPE
            ORDER BY YEAR
        ) AS prev_year_count,

        incident_count - LAG(incident_count) OVER (
            PARTITION BY NEIGHBOURHOOD, CRIME_TYPE
            ORDER BY YEAR
        ) AS yoy_change,

        -- Rank neighbourhoods by crime count within each year
        RANK() OVER (
            PARTITION BY YEAR
            ORDER BY incident_count DESC
        ) AS neighbourhood_rank_by_year

    FROM base
)

SELECT
    NEIGHBOURHOOD,
    YEAR,
    CRIME_TYPE,
    incident_count,
    prev_year_count,
    yoy_change,
    ROUND(
        CASE
            WHEN prev_year_count IS NULL OR prev_year_count = 0 THEN NULL
            ELSE (yoy_change / prev_year_count) * 100
        END, 2
    ) AS yoy_pct_change,
    neighbourhood_rank_by_year
FROM with_window_functions
ORDER BY YEAR, neighbourhood_rank_by_year