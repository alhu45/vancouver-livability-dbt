{{ config(materialized='table') }}

WITH stop_trip_counts AS (
    SELECT
        s.neighbourhood,
        s."stop_id",
        s."stop_name",
        s."stop_lat",
        s."stop_lon",
        s."wheelchair_boarding",
        r."route_short_name",
        r."route_long_name",
        r."route_type",
        COUNT(DISTINCT t."trip_id")        AS total_trips,
        COUNT(DISTINCT r."route_id")       AS total_routes_served,
        COUNT(DISTINCT st."arrival_time")  AS total_arrivals
    FROM {{ ref('stg_transit_stoptimes') }}  st
    JOIN {{ ref('stg_transit_stops') }}       s  ON st."stop_id"  = s."stop_id"
    JOIN {{ ref('stg_transit_trips') }}       t  ON st."trip_id"  = t."trip_id"
    JOIN {{ ref('stg_transit_routes') }}      r  ON t."route_id"  = r."route_id"
    WHERE s.neighbourhood != 'Outside Vancouver'
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
)

SELECT
    neighbourhood,
    "stop_id",
    "stop_name",
    "stop_lat",
    "stop_lon",
    "wheelchair_boarding",
    "route_short_name",
    "route_long_name",
    "route_type",
    total_trips,
    total_routes_served,
    total_arrivals,
    RANK() OVER (
        PARTITION BY neighbourhood
        ORDER BY total_trips DESC
    ) AS rank_within_neighbourhood,
    RANK() OVER (
        ORDER BY total_trips DESC
    ) AS citywide_stop_rank,
    ROUND(
        total_trips * 100.0 /
        NULLIF(SUM(total_trips) OVER (PARTITION BY neighbourhood), 0), 2
    ) AS pct_of_neighbourhood_trips,
    ROUND(
        total_trips * 100.0 /
        NULLIF(SUM(total_trips) OVER (), 0), 2
    ) AS pct_of_city_trips
FROM stop_trip_counts
ORDER BY neighbourhood, total_trips DESC