{{ config(
    materialized='table',
    schema='GOLD'
) }}

WITH cleaned AS (
    SELECT
        "Neighbourhood"     AS neighbourhood,
        "Property_Type"     AS property_type,
        "Date"              AS date,
        CAST(
            REPLACE(REPLACE(PRICE, '$', ''), ',', '') AS FLOAT
        ) AS price
    FROM {{ ref('stg_housing') }}
    WHERE PRICE IS NOT NULL
),

aggregated AS (
    SELECT
        Neighbourhood,
        Property_Type,
        Date,
        COUNT(*)                    AS listing_count,
        ROUND(AVG(price), 2)        AS avg_price,
        ROUND(MIN(price), 2)        AS min_price,
        ROUND(MAX(price), 2)        AS max_price
    FROM cleaned
    GROUP BY Neighbourhood, Property_Type, Date
),

with_window_functions AS (
    SELECT
        Neighbourhood,
        Property_Type,
        Date,
        listing_count,
        avg_price,
        min_price,
        max_price,

        -- Rank neighbourhoods by avg price within each property type
        RANK() OVER (
            PARTITION BY Property_Type, Date
            ORDER BY avg_price DESC
        ) AS price_rank_in_property_type,

        -- Month-over-month price change per neighbourhood and property type
        LAG(avg_price) OVER (
            PARTITION BY Neighbourhood, Property_Type
            ORDER BY Date
        ) AS prev_month_avg_price,

        avg_price - LAG(avg_price) OVER (
            PARTITION BY Neighbourhood, Property_Type
            ORDER BY Date
        ) AS mom_price_change,

        -- Each neighbourhood's avg price vs overall avg for that property type
        ROUND(
            AVG(avg_price) OVER (
                PARTITION BY Property_Type, Date
            ), 2
        ) AS market_avg_price

    FROM aggregated
)

SELECT
    Neighbourhood,
    Property_Type,
    Date,
    listing_count,
    avg_price,
    min_price,
    max_price,
    prev_month_avg_price,
    mom_price_change,
    ROUND(
        CASE
            WHEN prev_month_avg_price IS NULL OR prev_month_avg_price = 0 THEN NULL
            ELSE (mom_price_change / prev_month_avg_price) * 100
        END, 2
    )                               AS mom_pct_change,
    market_avg_price,

    -- How much above/below market average this neighbourhood is
    ROUND(avg_price - market_avg_price, 2) AS vs_market_avg,
    price_rank_in_property_type

FROM with_window_functions
ORDER BY Date, Property_Type, price_rank_in_property_type