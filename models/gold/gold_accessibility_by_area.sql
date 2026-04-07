{{ config(materialized='table') }}

WITH accessibility_counts AS (
    SELECT
        s.neighbourhood,
        COUNT(DISTINCT s."stop_id")                                             AS total_stops,
        SUM(CASE WHEN s."wheelchair_boarding" = 1 THEN 1 ELSE 0 END)           AS accessible_stops,
        SUM(CASE WHEN s."wheelchair_boarding" = 2 THEN 1 ELSE 0 END)           AS inaccessible_stops,
        SUM(CASE WHEN s."wheelchair_boarding" = 0 THEN 1 ELSE 0 END)           AS unknown_accessibility,
        ROUND(
            SUM(CASE WHEN s."wheelchair_boarding" = 1 THEN 1 ELSE 0 END) * 100.0
            / NULLIF(COUNT(DISTINCT s."stop_id"), 0), 2
        )                                                                       AS pct_accessible,
        COUNT(DISTINCT t."trip_id")                                             AS total_trips,
        COUNT(DISTINCT r."route_id")                                            AS total_routes,
        CASE
            WHEN ROUND(
                SUM(CASE WHEN s."wheelchair_boarding" = 1 THEN 1 ELSE 0 END) * 100.0
                / NULLIF(COUNT(DISTINCT s."stop_id"), 0), 2
            ) >= 75 THEN 'High Accessibility'
            WHEN ROUND(
                SUM(CASE WHEN s."wheelchair_boarding" = 1 THEN 1 ELSE 0 END) * 100.0
                / NULLIF(COUNT(DISTINCT s."stop_id"), 0), 2
            ) >= 50 THEN 'Medium Accessibility'
            ELSE 'Low Accessibility'
        END                                                                     AS accessibility_rating
    FROM {{ ref('stg_transit_stoptimes') }}  st
    JOIN {{ ref('stg_transit_stops') }}       s  ON st."stop_id"  = s."stop_id"
    JOIN {{ ref('stg_transit_trips') }}       t  ON st."trip_id"  = t."trip_id"
    JOIN {{ ref('stg_transit_routes') }}      r  ON t."route_id"  = r."route_id"
    WHERE s.neighbourhood != 'Outside Vancouver'
    GROUP BY 1
)

SELECT
    neighbourhood,
    total_stops,
    accessible_stops,
    inaccessible_stops,
    unknown_accessibility,
    pct_accessible,
    total_trips,
    total_routes,
    accessibility_rating,
    RANK() OVER (
        ORDER BY pct_accessible DESC
    ) AS accessibility_rank,
    ROUND(
        pct_accessible - AVG(pct_accessible) OVER (), 2
    ) AS vs_city_avg_accessibility,
    SUM(accessible_stops) OVER (
        ORDER BY pct_accessible DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_accessible_stops,
    ROUND(
        accessible_stops * 100.0 /
        NULLIF(SUM(accessible_stops) OVER (), 0), 2
    ) AS pct_of_city_accessible_stops
FROM accessibility_counts
ORDER BY pct_accessible DESC