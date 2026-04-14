{{ config(
    materialized='table',
    schema='GOLD'
) }}

WITH housing AS (
    SELECT NEIGHBOURHOOD, ROUND(AVG(AVG_PRICE, 2)) AS average_housing_price
    FROM {{ ref('gold_housing_summary')}}
    GROUP BY NEIGHBOURHOOD
),

accessibility AS (
    SELECT NEIGHBOURHOOD, PCT_ACCESSIBLE, TOTAL_STOPS, TOTAL_ROUTES, ACCESSIBILITY_RANK
    FROM {{ ref('gold_accessibility_by_area')}}
),

route_coverage AS (
    SELECT
        NEIGHBOURHOOD,
        MAX(TOTAL_ROUTES_IN_NEIGHBOURHOOD)  AS total_routes,
        SUM(TOTAL_TRIPS)                    AS total_trips
    FROM {{ ref('gold_route_coverage') }}
    GROUP BY NEIGHBOURHOOD
),

service_freq AS (
    SELECT
        NEIGHBOURHOOD,
        ROUND(AVG(TOTAL_TRIPS), 2)          AS avg_weekly_trips,
        ROUND(AVG(PCT_WEEKEND_COVERAGE), 2) AS avg_weekend_coverage
    FROM {{ ref('gold_service_frequency') }}
    GROUP BY NEIGHBOURHOOD
),

stop_activity AS (
    SELECT
        NEIGHBOURHOOD,
        SUM(TOTAL_ARRIVALS)                 AS total_arrivals,
        COUNT(DISTINCT stop_id)             AS total_stops
    FROM {{ ref('gold_transit_stop_activity') }}
    GROUP BY NEIGHBOURHOOD
),

crime_summary AS (
    SELECT
        NEIGHBOURHOOD,
        SUM(INCIDENT_COUNT) AS total_incidents
    FROM {{ ref('gold_crime_neighbourhood_summary') }}
    WHERE YEAR = (SELECT MAX(YEAR) FROM {{ ref('gold_crime_neighbourhood_summary') }})
    GROUP BY NEIGHBOURHOOD
),

crime_patterns AS (
    SELECT
        NEIGHBOURHOOD,
        COUNT(*)                                                AS total_months,
        SUM(CASE WHEN IS_ANOMALY_CANDIDATE THEN 1 ELSE 0 END)  AS anomaly_months,
        ROUND(
            SUM(CASE WHEN IS_ANOMALY_CANDIDATE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
        )                                                       AS anomaly_rate_pct
    FROM {{ ref('gold_crime_patterns') }}
    GROUP BY NEIGHBOURHOOD
),

crime_hotspots AS (
    SELECT
        NEIGHBOURHOOD,
        MAX(NEIGHBOURHOOD_TOTAL_INCIDENTS)  AS neighbourhood_total_incidents,
        COUNT(DISTINCT BLOCK)               AS total_blocks_with_crime
    FROM {{ ref('gold_crime_hotspots') }}
    GROUP BY NEIGHBOURHOOD
),

joined AS (
    SELECT
        a.NEIGHBOURHOOD,
 
        -- Housing
        h.avg_housing_price,
 
        -- Transit
        a.PCT_ACCESSIBLE,
        a.TOTAL_STOPS,
        a.TOTAL_ROUTES,
        rc.total_trips,
        sf.avg_weekly_trips,
        sf.avg_weekend_coverage,
        sa.total_arrivals,
 
        -- Crime
        cs.total_incidents,
        cp.anomaly_rate_pct,
        ch.neighbourhood_total_incidents,
        ch.total_blocks_with_crime
 
    FROM accessibility a
    LEFT JOIN housing           h   ON a.NEIGHBOURHOOD = h.NEIGHBOURHOOD
    LEFT JOIN route_coverage    rc  ON a.NEIGHBOURHOOD = rc.NEIGHBOURHOOD
    LEFT JOIN service_freq      sf  ON a.NEIGHBOURHOOD = sf.NEIGHBOURHOOD
    LEFT JOIN stop_activity     sa  ON a.NEIGHBOURHOOD = sa.NEIGHBOURHOOD
    LEFT JOIN crime_summary     cs  ON a.NEIGHBOURHOOD = cs.NEIGHBOURHOOD
    LEFT JOIN crime_patterns    cp  ON a.NEIGHBOURHOOD = cp.NEIGHBOURHOOD
    LEFT JOIN crime_hotspots    ch  ON a.NEIGHBOURHOOD = ch.NEIGHBOURHOOD
),

-- NORMALIZE each metric to 0-100 using min-max scaling
normalized AS (
    SELECT
        NEIGHBOURHOOD,
        avg_housing_price,
        PCT_ACCESSIBLE,
        total_trips,
        total_incidents,
        anomaly_rate_pct,
 
        -- Housing affordability: lower price = higher score
        ROUND(
            100 - (
                (avg_housing_price - MIN(avg_housing_price) OVER()) /
                NULLIF(MAX(avg_housing_price) OVER() - MIN(avg_housing_price) OVER(), 0)
            ) * 100, 2
        ) AS housing_affordability_score,
 
        -- Transit accessibility: higher PCT_ACCESSIBLE = higher score
        ROUND(
            (
                (PCT_ACCESSIBLE - MIN(PCT_ACCESSIBLE) OVER()) /
                NULLIF(MAX(PCT_ACCESSIBLE) OVER() - MIN(PCT_ACCESSIBLE) OVER(), 0)
            ) * 100, 2
        ) AS transit_accessibility_score,
 
        -- Transit coverage: higher total_trips = higher score
        ROUND(
            (
                (total_trips - MIN(total_trips) OVER()) /
                NULLIF(MAX(total_trips) OVER() - MIN(total_trips) OVER(), 0)
            ) * 100, 2
        ) AS transit_coverage_score,
 
        -- Crime safety: lower incidents = higher score
        ROUND(
            100 - (
                (total_incidents - MIN(total_incidents) OVER()) /
                NULLIF(MAX(total_incidents) OVER() - MIN(total_incidents) OVER(), 0)
            ) * 100, 2
        ) AS crime_safety_score,
 
        -- Anomaly safety: lower anomaly rate = higher score
        ROUND(
            100 - (
                (anomaly_rate_pct - MIN(anomaly_rate_pct) OVER()) /
                NULLIF(MAX(anomaly_rate_pct) OVER() - MIN(anomaly_rate_pct) OVER(), 0)
            ) * 100, 2
        ) AS anomaly_safety_score
 
    FROM joined
),
 
-- ────────────────────────────────────────────────────────────
-- COMPOSITE SCORE weighted average
--   Crime Safety          30%
--   Housing Affordability 25%
--   Transit Accessibility 25%
--   Transit Coverage      20%
-- ────────────────────────────────────────────────────────────
final AS (
    SELECT
        NEIGHBOURHOOD,
        avg_housing_price,
        PCT_ACCESSIBLE,
        total_trips,
        total_incidents,
        anomaly_rate_pct,
        housing_affordability_score,
        transit_accessibility_score,
        transit_coverage_score,
        crime_safety_score,
        anomaly_safety_score,
 
        ROUND(
            (crime_safety_score      * 0.30) +
            (housing_affordability_score * 0.25) +
            (transit_accessibility_score * 0.25) +
            (transit_coverage_score  * 0.20),
        2) AS composite_livability_score,
 
        -- Final ranking across all neighbourhoods
        RANK() OVER (
            ORDER BY (
                (crime_safety_score          * 0.30) +
                (housing_affordability_score * 0.25) +
                (transit_accessibility_score * 0.25) +
                (transit_coverage_score      * 0.20)
            ) DESC
        ) AS livability_rank
 
    FROM normalized
)
 
SELECT
    NEIGHBOURHOOD,
    composite_livability_score,
    livability_rank,
    housing_affordability_score,
    transit_accessibility_score,
    transit_coverage_score,
    crime_safety_score,
    anomaly_safety_score,
    avg_housing_price,
    PCT_ACCESSIBLE          AS pct_transit_accessible,
    total_trips             AS total_transit_trips,
    total_incidents         AS total_crime_incidents,
    anomaly_rate_pct        AS crime_anomaly_rate_pct
FROM final
ORDER BY livability_rank