-- ============================================================
-- GOLD MODEL: Route Coverage by Neighbourhood
-- PURPOSE: Shows which routes serve each neighbourhood and
--          how well connected each area is to the rest of Vancouver
-- JOINS: routes + trips + stop_times + stops
-- ============================================================

WITH route_neighbourhood_counts AS (
    -- STEP 1: Count routes, trips and stops per neighbourhood
    -- route_type values (GTFS standard):
    --   0 = Tram/Streetcar
    --   1 = Subway/Metro
    --   2 = Rail
    --   3 = Bus
    --   4 = Ferry

    SELECT
        s.neighbourhood,
        r.route_id,
        r.route_short_name,
        r.route_long_name,

        -- Translate numeric route_type into readable label
        CASE
            WHEN r.route_type = 0 THEN 'Tram/Streetcar'
            WHEN r.route_type = 1 THEN 'Subway/Metro'
            WHEN r.route_type = 2 THEN 'Rail'
            WHEN r.route_type = 3 THEN 'Bus'
            WHEN r.route_type = 4 THEN 'Ferry'
            ELSE 'Other'
        END AS route_type_name,

        COUNT(DISTINCT t.trip_id)  AS total_trips,
        COUNT(DISTINCT s.stop_id)  AS unique_stops_served,

        -- Geographic spread of the route within this neighbourhood
        -- MIN/MAX lat and lon shows how far the route reaches
        MIN(s.stop_lat)            AS southernmost_lat,
        MAX(s.stop_lat)            AS northernmost_lat,
        MIN(s.stop_lon)            AS westernmost_lon,
        MAX(s.stop_lon)            AS easternmost_lon,

        -- Count how many stops on this route are wheelchair accessible
        SUM(CASE WHEN s.wheelchair_boarding = 1
                 THEN 1 ELSE 0 END) AS accessible_stops_on_route

    FROM {{ ref('stg_transit_routes') }}     r
    JOIN {{ ref('stg_transit_trips') }}      t  ON r.route_id  = t.route_id
    JOIN {{ ref('stg_transit_stoptimes') }}  st ON t.trip_id   = st.trip_id
    JOIN {{ ref('stg_transit_stops') }}      s  ON st.stop_id  = s.stop_id

    WHERE s.neighbourhood != 'Outside Vancouver'

    GROUP BY 1, 2, 3, 4, 5
)

-- STEP 2: Add window functions to measure how well connected
-- each neighbourhood is compared to the rest of Vancouver

SELECT
    neighbourhood,
    route_id,
    route_short_name,
    route_long_name,
    route_type_name,
    total_trips,
    unique_stops_served,
    southernmost_lat,
    northernmost_lat,
    westernmost_lon,
    easternmost_lon,
    accessible_stops_on_route,

    -- WINDOW FUNCTION 1: Count total distinct routes per neighbourhood
    -- COUNT OVER PARTITION = count routes without collapsing rows
    -- Livability use: more routes = better connected neighbourhood
    COUNT(DISTINCT route_id) OVER (
        PARTITION BY neighbourhood
    ) AS total_routes_in_neighbourhood,

    -- WINDOW FUNCTION 2: Rank neighbourhoods by number of routes
    -- Uses DENSE_RANK so tied neighbourhoods get same rank
    -- Livability use: low rank = transit desert = livability concern
    DENSE_RANK() OVER (
        ORDER BY COUNT(DISTINCT route_id) OVER (PARTITION BY neighbourhood) DESC
    ) AS route_diversity_rank,

    -- WINDOW FUNCTION 3: Rank neighbourhoods by total trips
    -- More trips = more frequent service = more livable
    -- Livability use: compare transit volume across neighbourhoods
    RANK() OVER (
        ORDER BY SUM(total_trips) OVER (PARTITION BY neighbourhood) DESC
    ) AS transit_volume_rank,

    -- WINDOW FUNCTION 4: How does this neighbourhood compare
    -- to city average number of routes
    -- Positive = above average routes, Negative = below average
    -- Livability use: negative neighbourhoods are underserved
    ROUND(
        COUNT(DISTINCT route_id) OVER (PARTITION BY neighbourhood) -
        AVG(COUNT(DISTINCT route_id)) OVER (), 2
    ) AS vs_city_avg_routes,

    -- WINDOW FUNCTION 5: What % of total city trips flow
    -- through this neighbourhood
    -- Livability use: shows which neighbourhoods are most transit-dependent
    -- and which ones are being overlooked by the transit system
    ROUND(
        SUM(total_trips) OVER (PARTITION BY neighbourhood) * 100.0 /
        NULLIF(SUM(total_trips) OVER (), 0), 2
    ) AS pct_of_city_transit_volume

FROM route_neighbourhood_counts
ORDER BY neighbourhood, total_trips DESC