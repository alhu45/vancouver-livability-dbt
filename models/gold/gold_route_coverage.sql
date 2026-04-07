{{ config(materialized='table') }}

WITH route_neighbourhood_counts AS (
    SELECT
        s.neighbourhood,
        r."route_id",
        r."route_short_name",
        r."route_long_name",
        CASE
            WHEN r."route_type" = 0 THEN 'Tram/Streetcar'
            WHEN r."route_type" = 1 THEN 'Subway/Metro'
            WHEN r."route_type" = 2 THEN 'Rail'
            WHEN r."route_type" = 3 THEN 'Bus'
            WHEN r."route_type" = 4 THEN 'Ferry'
            ELSE 'Other'
        END AS route_type_name,
        COUNT(DISTINCT t."trip_id")                                     AS total_trips,
        COUNT(DISTINCT s."stop_id")                                     AS unique_stops_served,
        MIN(s."stop_lat")                                               AS southernmost_lat,
        MAX(s."stop_lat")                                               AS northernmost_lat,
        MIN(s."stop_lon")                                               AS westernmost_lon,
        MAX(s."stop_lon")                                               AS easternmost_lon,
        SUM(CASE WHEN s."wheelchair_boarding" = 1 THEN 1 ELSE 0 END)   AS accessible_stops_on_route
    FROM {{ ref('stg_transit_routes') }}     r
    JOIN {{ ref('stg_transit_trips') }}      t  ON r."route_id" = t."route_id"
    JOIN {{ ref('stg_transit_stoptimes') }}  st ON t."trip_id"  = st."trip_id"
    JOIN {{ ref('stg_transit_stops') }}      s  ON st."stop_id" = s."stop_id"
    WHERE s.neighbourhood != 'Outside Vancouver'
    GROUP BY 1, 2, 3, 4, 5
),

-- Pre-compute distinct route count per neighbourhood
-- so we can safely AVG it in a window function without nesting aggregates
neighbourhood_route_counts AS (
    SELECT
        neighbourhood,
        COUNT(DISTINCT "route_id") AS routes_in_neighbourhood
    FROM route_neighbourhood_counts
    GROUP BY neighbourhood
),

city_avg AS (
    SELECT AVG(routes_in_neighbourhood) AS avg_routes_city
    FROM neighbourhood_route_counts
)

SELECT
    r.neighbourhood,
    r."route_id",
    r."route_short_name",
    r."route_long_name",
    r.route_type_name,
    r.total_trips,
    r.unique_stops_served,
    r.southernmost_lat,
    r.northernmost_lat,
    r.westernmost_lon,
    r.easternmost_lon,
    r.accessible_stops_on_route,

    n.routes_in_neighbourhood                                           AS total_routes_in_neighbourhood,

    DENSE_RANK() OVER (
        ORDER BY n.routes_in_neighbourhood DESC
    )                                                                   AS route_diversity_rank,

    RANK() OVER (
        ORDER BY SUM(r.total_trips) OVER (PARTITION BY r.neighbourhood) DESC
    )                                                                   AS transit_volume_rank,

    ROUND(n.routes_in_neighbourhood - c.avg_routes_city, 2)            AS vs_city_avg_routes,

    ROUND(
        SUM(r.total_trips) OVER (PARTITION BY r.neighbourhood) * 100.0 /
        NULLIF(SUM(r.total_trips) OVER (), 0), 2
    )                                                                   AS pct_of_city_transit_volume

FROM route_neighbourhood_counts  r
JOIN neighbourhood_route_counts  n ON r.neighbourhood = n.neighbourhood
CROSS JOIN city_avg              c
ORDER BY r.neighbourhood, r.total_trips DESC