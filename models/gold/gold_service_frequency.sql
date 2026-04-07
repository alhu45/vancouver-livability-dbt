-- ============================================================
-- GOLD MODEL: Service Frequency by Neighbourhood
-- PURPOSE: Shows weekday vs weekend transit coverage per
--          neighbourhood — key for car-free livability
-- JOINS: trips + routes + calendar + stop_times + stops
-- ============================================================

WITH service_counts AS (
    -- STEP 1: Count weekday and weekend trips per neighbourhood per route
    -- The calendar table tells us which days each service runs
    -- We use CASE WHEN to split trips into weekday vs weekend buckets

    SELECT
        s.neighbourhood,
        r.route_short_name,
        r.route_long_name,
        r.route_type,

        COUNT(DISTINCT t.trip_id) AS total_trips,

        -- CASE WHEN checks if the day column = 1 (runs that day)
        -- SUM adds up all the 1s to get total trips on that day
        SUM(CASE WHEN c.monday    = 1 THEN 1 ELSE 0 END) AS monday_trips,
        SUM(CASE WHEN c.tuesday   = 1 THEN 1 ELSE 0 END) AS tuesday_trips,
        SUM(CASE WHEN c.wednesday = 1 THEN 1 ELSE 0 END) AS wednesday_trips,
        SUM(CASE WHEN c.thursday  = 1 THEN 1 ELSE 0 END) AS thursday_trips,
        SUM(CASE WHEN c.friday    = 1 THEN 1 ELSE 0 END) AS friday_trips,
        SUM(CASE WHEN c.saturday  = 1 THEN 1 ELSE 0 END) AS saturday_trips,
        SUM(CASE WHEN c.sunday    = 1 THEN 1 ELSE 0 END) AS sunday_trips,

        -- Total weekday trips = any trip that runs Mon-Fri
        SUM(CASE WHEN c.monday + c.tuesday + c.wednesday +
                 c.thursday + c.friday > 0 THEN 1 ELSE 0 END) AS total_weekday_trips,

        -- Total weekend trips = any trip that runs Sat or Sun
        SUM(CASE WHEN c.saturday + c.sunday > 0
                 THEN 1 ELSE 0 END)                           AS total_weekend_trips,

        -- Weekend coverage % = what portion of trips run on weekends
        -- NULLIF prevents division by zero errors
        ROUND(
            SUM(CASE WHEN c.saturday + c.sunday > 0 THEN 1 ELSE 0 END) * 100.0
            / NULLIF(COUNT(DISTINCT t.trip_id), 0), 2
        ) AS pct_weekend_coverage

    FROM {{ ref('stg_transit_trips') }}      t
    JOIN {{ ref('stg_transit_routes') }}     r  ON t.route_id   = r.route_id
    JOIN {{ ref('stg_transit_calendar') }}   c  ON t.service_id = c.service_id
    JOIN {{ ref('stg_transit_stoptimes') }}  st ON t.trip_id    = st.trip_id
    JOIN {{ ref('stg_transit_stops') }}      s  ON st.stop_id   = s.stop_id

    WHERE s.neighbourhood != 'Outside Vancouver'

    GROUP BY 1, 2, 3, 4
)

-- STEP 2: Add window functions to compare neighbourhoods to each other
-- and to the city average

SELECT
    neighbourhood,
    route_short_name,
    route_long_name,
    route_type,
    total_trips,
    monday_trips,
    tuesday_trips,
    wednesday_trips,
    thursday_trips,
    friday_trips,
    saturday_trips,
    sunday_trips,
    total_weekday_trips,
    total_weekend_trips,
    pct_weekend_coverage,

    -- WINDOW FUNCTION 1: Rank neighbourhoods by weekend coverage %
    -- Higher % = better weekend service = more livable for car-free residents
    -- Livability use: identify which neighbourhoods lose transit on weekends
    RANK() OVER (
        ORDER BY pct_weekend_coverage DESC
    ) AS weekend_coverage_rank,

    -- WINDOW FUNCTION 2: How does this neighbourhood compare to city average weekend trips
    -- AVG(total_weekend_trips) OVER () = average weekend trips across ALL neighbourhoods
    -- Positive number = above average, Negative = below average
    -- Livability use: neighbourhoods below 0 are underserved on weekends
    ROUND(
        total_weekend_trips - AVG(total_weekend_trips) OVER (), 2
    ) AS vs_city_avg_weekend_trips,

    -- WINDOW FUNCTION 3: Rank neighbourhoods by weekday trips
    -- Livability use: shows which neighbourhoods have strongest commuter service
    RANK() OVER (
        ORDER BY total_weekday_trips DESC
    ) AS weekday_rank,

    -- WINDOW FUNCTION 4: Weekend gap compared to city average
    -- (weekday - weekend) = the gap for this neighbourhood
    -- Minus AVG gap across city = how much worse/better than average
    -- Livability use: large positive number = neighbourhood loses a lot of
    -- transit on weekends compared to other Vancouver neighbourhoods
    ROUND(
        (total_weekday_trips - total_weekend_trips) -
        AVG(total_weekday_trips - total_weekend_trips) OVER (), 2
    ) AS weekend_gap_vs_city_avg

FROM service_counts
ORDER BY neighbourhood, total_trips DESC