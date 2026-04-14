{{ config(
    materialized='table',
    schema='GOLD'
) }}

WITH base AS (
    SELECT
        NEIGHBOURHOOD,
        YEAR,
        DATE                        AS month,
        CRIME_TYPE,
        COUNT(*)                    AS monthly_incidents
    FROM {{ ref('stg_crime') }}
    GROUP BY NEIGHBOURHOOD, YEAR, DATE, CRIME_TYPE
),

with_window_functions AS (
    SELECT
        NEIGHBOURHOOD,
        YEAR,
        month,
        CRIME_TYPE,
        monthly_incidents,

        -- Month-over-month change per neighbourhood
        LAG(monthly_incidents) OVER (
            PARTITION BY NEIGHBOURHOOD, CRIME_TYPE
            ORDER BY YEAR, month
        ) AS prev_month_incidents,

        monthly_incidents - LAG(monthly_incidents) OVER (
            PARTITION BY NEIGHBOURHOOD, CRIME_TYPE
            ORDER BY YEAR, month
        ) AS mom_change,

        -- 3-month rolling average (AI agent uses this as anomaly baseline)
        ROUND(
            AVG(monthly_incidents) OVER (
                PARTITION BY NEIGHBOURHOOD, CRIME_TYPE
                ORDER BY YEAR, month
                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
            ), 2
        ) AS rolling_3m_avg,

        -- 6-month rolling average for longer-term trend
        ROUND(
            AVG(monthly_incidents) OVER (
                PARTITION BY NEIGHBOURHOOD, CRIME_TYPE
                ORDER BY YEAR, month
                ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
            ), 2
        ) AS rolling_6m_avg

    FROM base
)

SELECT
    NEIGHBOURHOOD,
    YEAR,
    month,
    CRIME_TYPE,
    monthly_incidents,
    prev_month_incidents,
    mom_change,
    rolling_3m_avg,
    rolling_6m_avg,

    -- Flag months where incidents exceed rolling average by more than 50%
    -- Used by the Isolation Forest AI agent as a pre-computed signal
    CASE
        WHEN rolling_3m_avg IS NULL THEN FALSE
        WHEN monthly_incidents > rolling_3m_avg * 1.5 THEN TRUE
        ELSE FALSE
    END AS is_anomaly_candidate

FROM with_window_functions
ORDER BY NEIGHBOURHOOD, YEAR, month