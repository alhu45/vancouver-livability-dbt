{{ config(
    materialized='table',
    schema='GOLD'
) }}

WITH base AS (
    SELECT
        BLOCK,
        NEIGHBOURHOOD,
        CRIME_TYPE,
        X,
        Y,
        COUNT(*) AS total_incidents
    FROM {{ ref('stg_crime') }}
    GROUP BY BLOCK, NEIGHBOURHOOD, CRIME_TYPE, X, Y
),

neighbourhood_totals AS (
    SELECT
        NEIGHBOURHOOD,
        SUM(total_incidents) AS neighbourhood_total_incidents
    FROM base
    GROUP BY NEIGHBOURHOOD
),

with_window_functions AS (
    SELECT
        b.BLOCK,
        b.NEIGHBOURHOOD,
        b.CRIME_TYPE,
        b.X,
        b.Y,
        b.total_incidents,
        nt.neighbourhood_total_incidents,

        -- Rank blocks by incident count within each neighbourhood
        RANK() OVER (
            PARTITION BY b.NEIGHBOURHOOD
            ORDER BY b.total_incidents DESC
        ) AS block_rank_in_neighbourhood,

        -- Each block's share of total neighbourhood incidents
        ROUND(
            (b.total_incidents / nt.neighbourhood_total_incidents) * 100, 2
        ) AS pct_of_neighbourhood_incidents

    FROM base b
    JOIN neighbourhood_totals nt
        ON b.NEIGHBOURHOOD = nt.NEIGHBOURHOOD
)

SELECT
    BLOCK,
    NEIGHBOURHOOD,
    CRIME_TYPE,
    X,
    Y,
    total_incidents,
    neighbourhood_total_incidents,
    block_rank_in_neighbourhood,
    pct_of_neighbourhood_incidents
FROM with_window_functions
ORDER BY NEIGHBOURHOOD, block_rank_in_neighbourhood