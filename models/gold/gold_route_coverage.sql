WITH route_neighbourhood_counts AS (
    SELECT
        s.neighbourhood,
        r.route_id,
        r.route_short_name,
        r.route_long_name,
        CASE
            WHEN r.route_type = 0 THEN 'Tram/Streetcar'
            WHEN r.route_type = 1 THEN 'Subway/Metro'
            WHEN r.route_type = 2 THEN 'Rail'
            WHEN r.route_type = 3 THEN 'Bus'
            WHEN r.route_type = 4 THEN 'Ferry'
            ELSE 'Other'
        END AS route_type_name,
        COUNT(DISTINCT t.trip_id)                                    AS total_trips,
        COUNT(DISTINCT s.stop_id)                                    AS unique_stops_served,
        MIN(s.stop_lat)                                              AS southernmost_lat,
        MAX(s.stop_lat)                                              AS northernmost_lat,
        MIN(s.stop_lon)                                              AS westernmost_lon,
        MAX(s.stop_lon)                                              AS easternmost_lon,
        SUM(CASE WHEN s.wheelchair_boarding = 1 THEN 1 ELSE 0 END)  AS accessible_stops_on_route
    FROM {{ ref('stg_transit_routes') }}     r
    JOIN {{ ref('stg_transit_trips') }}      t  ON r.route_id  = t.route_id
    JOIN {{ ref('stg_transit_stoptimes') }}  st ON t.trip_id   = st.trip_id
    JOIN {{ ref('stg_transit_stops') }}      s  ON st.stop_id  = s.stop_id
    WHERE s.neighbourhood != 'Outside Vancouver'
    GROUP BY
        s.neighbourhood,
        r.route_id,
        r.route_short_name,
        r.route_long_name,
        route_type_name
),

neighbourhood_aggregates AS (
    SELECT
        neighbourhood,
        COUNT(DISTINCT route_id)  AS total_routes_in_neighbourhood,
        SUM(total_trips)          AS neighbourhood_total_trips
    FROM route_neighbourhood_counts
    GROUP BY neighbourhood
),

city_aggregates AS (
    SELECT
        AVG(total_routes_in_neighbourhood)   AS city_avg_routes,
        SUM(neighbourhood_total_trips)       AS city_total_trips
    FROM neighbourhood_aggregates
)

SELECT
    rnc.neighbourhood,
    rnc.route_id,
    rnc.route_short_name,
    rnc.route_long_name,
    rnc.route_type_name,
    rnc.total_trips,
    rnc.unique_stops_served,
    rnc.southernmost_lat,
    rnc.northernmost_lat,
    rnc.westernmost_lon,
    rnc.easternmost_lon,
    rnc.accessible_stops_on_route,
    na.total_routes_in_neighbourhood,
    DENSE_RANK() OVER (ORDER BY na.total_routes_in_neighbourhood DESC)  AS route_diversity_rank,
    RANK()       OVER (ORDER BY na.neighbourhood_total_trips DESC)      AS transit_volume_rank,
    ROUND(na.total_routes_in_neighbourhood - ca.city_avg_routes, 2)     AS vs_city_avg_routes,
    ROUND(na.neighbourhood_total_trips * 100.0
          / NULLIF(ca.city_total_trips, 0), 2)                          AS pct_of_city_transit_volume
FROM route_neighbourhood_counts  rnc
JOIN neighbourhood_aggregates    na  ON rnc.neighbourhood = na.neighbourhood
CROSS JOIN city_aggregates       ca
ORDER BY rnc.neighbourhood, rnc.total_trips DESC