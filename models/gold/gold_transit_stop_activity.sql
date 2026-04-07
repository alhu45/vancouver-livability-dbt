-- ============================================================
-- GOLD MODEL: Transit Stop Activity by Neighbourhood
-- PURPOSE: Shows how busy each transit stop is within each
--          Vancouver neighbourhood and across the whole city
-- JOINS: stop_times + stops + trips + routes
-- ============================================================

-- CTEs (Common Table Expressions) let us build the query step by step
-- Think of each CTE as a temporary table we can reference below

WITH stop_trip_counts AS (
    -- STEP 1: Count how many trips and routes serve each stop
    -- We join 4 tables together to get the full picture:
    --   stop_times → tells us which stops each trip visits
    --   stops      → gives us stop name, location, neighbourhood
    --   trips      → links stop_times to routes
    --   routes     → gives us route name and type

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

        -- COUNT(DISTINCT) avoids double counting the same trip or route
        COUNT(DISTINCT t.trip_id)        AS total_trips,
        COUNT(DISTINCT r.route_id)       AS total_routes_served,
        COUNT(DISTINCT st.arrival_time)  AS total_arrivals

    FROM {{ ref('stg_transit_stoptimes') }}  st
    JOIN {{ ref('stg_transit_stops') }}       s  ON st.stop_id  = s.stop_id
    JOIN {{ ref('stg_transit_trips') }}       t  ON st.trip_id  = t.trip_id
    JOIN {{ ref('stg_transit_routes') }}      r  ON t.route_id  = r.route_id

    -- Only include stops that fall within Vancouver neighbourhoods
    WHERE s.neighbourhood != 'Outside Vancouver'

    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
)

-- STEP 2: Add window functions on top of the aggregated data
-- Window functions let us compare each row to other rows
-- without collapsing the results like GROUP BY does

SELECT
    neighbourhood,
    stop_id,
    stop_name,
    stop_lat,
    stop_lon,
    wheelchair_boarding,
    route_short_name,
    route_long_name,
    route_type,
    total_trips,
    total_routes_served,
    total_arrivals,

    -- WINDOW FUNCTION 1: Rank each stop within its neighbourhood
    -- PARTITION BY neighbourhood = reset ranking for each neighbourhood
    -- ORDER BY total_trips DESC = busiest stop gets rank 1
    -- Livability use: find the top stops in each neighbourhood
    RANK() OVER (
        PARTITION BY neighbourhood
        ORDER BY total_trips DESC
    ) AS rank_within_neighbourhood,

    -- WINDOW FUNCTION 2: Rank each stop across ALL of Vancouver
    -- No PARTITION BY = looks at all rows together
    -- Livability use: find the most important stops citywide
    RANK() OVER (
        ORDER BY total_trips DESC
    ) AS citywide_stop_rank,

    -- WINDOW FUNCTION 3: What % of this neighbourhood's trips does this stop handle
    -- SUM(total_trips) OVER (PARTITION BY neighbourhood) = total trips in that neighbourhood
    -- Dividing this stop's trips by neighbourhood total gives share %
    -- Livability use: a neighbourhood dependent on 1 stop is fragile
    ROUND(
        total_trips * 100.0 /
        NULLIF(SUM(total_trips) OVER (PARTITION BY neighbourhood), 0), 2
    ) AS pct_of_neighbourhood_trips,

    -- WINDOW FUNCTION 4: What % of ALL city trips does this stop handle
    -- SUM(total_trips) OVER () = grand total across every stop in the city
    -- Livability use: identify stops critical to the whole city's transit system
    ROUND(
        total_trips * 100.0 /
        NULLIF(SUM(total_trips) OVER (), 0), 2
    ) AS pct_of_city_trips

FROM stop_trip_counts
ORDER BY neighbourhood, total_trips DESC