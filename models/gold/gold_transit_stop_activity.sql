{{ config(materialized='table') }}

WITH stop_trip_counts AS (
    SELECT
        s.neighbourhood,
        s.stop_id,
        s.stop_name,
        s.stop_lat,
        s.stop_lon,
        s.wheelchair_boarding,
        r.route_short_name,
        r.route_long_name,
        r.route_type,
        COUNT(DISTINCT t.trip_id)        AS total_trips,
        COUNT(DISTINCT r.route_id)       AS total_routes_served,
        COUNT(DISTINCT st.arrival_time)  AS total_arrivals
    FROM {{ ref('stg_transit_stoptimes') }}  st
    JOIN {{ ref('stg_transit_stops') }}       s  ON st."stop_id"  = s."stop_id"
    JOIN {{ ref('stg_transit_trips') }}       t  ON st."trip_id"  = t."trip_id"
    JOIN {{ ref('stg_transit_routes') }}      r  ON t."route_id"  = r."route_id"
    WHERE s.neighbourhood != 'Outside Vancouver'
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
)

SELECT * FROM stop_trip_counts