-- ============================================================
-- GOLD MODEL: Accessibility Score by Neighbourhood
-- PURPOSE: Measures wheelchair accessible stops per neighbourhood
--          Key equity metric for Vancouver livability analysis
-- JOINS: stops + stop_times + trips + routes
-- ============================================================

WITH accessibility_counts AS (
    -- STEP 1: Count accessible vs inaccessible stops per neighbourhood
    -- wheelchair_boarding values:
    --   0 = no accessibility info available
    --   1 = wheelchair accessible
    --   2 = not wheelchair accessible

    SELECT
        s.neighbourhood,

        -- Count every unique stop in this neighbourhood
        COUNT(DISTINCT s.stop_id) AS total_stops,

        -- Count only accessible stops (wheelchair_boarding = 1)
        SUM(CASE WHEN s.wheelchair_boarding = 1
                 THEN 1 ELSE 0 END) AS accessible_stops,

        -- Count stops confirmed NOT accessible (wheelchair_boarding = 2)
        SUM(CASE WHEN s.wheelchair_boarding = 2
                 THEN 1 ELSE 0 END) AS inaccessible_stops,

        -- Count stops with unknown accessibility (wheelchair_boarding = 0)
        SUM(CASE WHEN s.wheelchair_boarding = 0
                 THEN 1 ELSE 0 END) AS unknown_accessibility,

        -- Calculate % of stops that are accessible
        -- NULLIF prevents division by zero if a neighbourhood has 0 stops
        ROUND(
            SUM(CASE WHEN s.wheelchair_boarding = 1
                     THEN 1 ELSE 0 END) * 100.0
            / NULLIF(COUNT(DISTINCT s.stop_id), 0), 2
        ) AS pct_accessible,

        COUNT(DISTINCT t.trip_id)  AS total_trips,
        COUNT(DISTINCT r.route_id) AS total_routes,

        -- Label each neighbourhood with a simple accessibility rating
        -- based on what % of its stops are accessible
        CASE
            WHEN ROUND(
                SUM(CASE WHEN s.wheelchair_boarding = 1
                         THEN 1 ELSE 0 END) * 100.0
                / NULLIF(COUNT(DISTINCT s.stop_id), 0), 2
            ) >= 75 THEN 'High Accessibility'
            WHEN ROUND(
                SUM(CASE WHEN s.wheelchair_boarding = 1
                         THEN 1 ELSE 0 END) * 100.0
                / NULLIF(COUNT(DISTINCT s.stop_id), 0), 2
            ) >= 50 THEN 'Medium Accessibility'
            ELSE 'Low Accessibility'
        END AS accessibility_rating

    FROM {{ ref('stg_transit_stops') }}      s
    JOIN {{ ref('stg_transit_stoptimes') }}  st ON s.stop_id  = st.stop_id
    JOIN {{ ref('stg_transit_trips') }}      t  ON st.trip_id = t.trip_id
    JOIN {{ ref('stg_transit_routes') }}     r  ON t.route_id = r.route_id

    WHERE s.neighbourhood != 'Outside Vancouver'

    GROUP BY 1
)

-- STEP 2: Add window functions to rank and compare neighbourhoods
-- on accessibility — the key equity metric for livability

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

    -- WINDOW FUNCTION 1: Rank neighbourhoods by accessibility %
    -- Rank 1 = most accessible neighbourhood in Vancouver
    -- Livability use: quickly see which neighbourhoods rank worst for
    -- disabled residents trying to use transit
    RANK() OVER (
        ORDER BY pct_accessible DESC
    ) AS accessibility_rank,

    -- WINDOW FUNCTION 2: How far is this neighbourhood from city average
    -- AVG(pct_accessible) OVER () = city average accessibility %
    -- Positive = above average, Negative = below average
    -- Livability use: neighbourhoods with negative values are equity concerns
    ROUND(
        pct_accessible - AVG(pct_accessible) OVER (), 2
    ) AS vs_city_avg_accessibility,

    -- WINDOW FUNCTION 3: Running total of accessible stops
    -- as we go from most to least accessible neighbourhood
    -- Livability use: shows how concentrated accessible stops are
    -- e.g. top 5 neighbourhoods might have 80% of all accessible stops
    SUM(accessible_stops) OVER (
        ORDER BY pct_accessible DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_accessible_stops,

    -- WINDOW FUNCTION 4: What % of ALL city accessi{{ config(materialized='table') }}

WITH accessibility_counts AS (
    SELECT
        s.neighbourhood,
        COUNT(DISTINCT s.stop_id) AS total_stops,
        SUM(CASE WHEN s.wheelchair_boarding = 1 THEN 1 ELSE 0 END) AS accessible_stops,
        SUM(CASE WHEN s.wheelchair_boarding = 2 THEN 1 ELSE 0 END) AS inaccessible_stops,
        SUM(CASE WHEN s.wheelchair_boarding = 0 THEN 1 ELSE 0 END) AS unknown_accessibility,
        ROUND(
            SUM(CASE WHEN s.wheelchair_boarding = 1 THEN 1 ELSE 0 END) * 100.0
            / NULLIF(COUNT(DISTINCT s.stop_id), 0), 2
        ) AS pct_accessible,
        COUNT(DISTINCT t.trip_id)  AS total_trips,
        COUNT(DISTINCT r.route_id) AS total_routes,
        CASE
            WHEN ROUND(
                SUM(CASE WHEN s.wheelchair_boarding = 1 THEN 1 ELSE 0 END) * 100.0
                / NULLIF(COUNT(DISTINCT s.stop_id), 0), 2
            ) >= 75 THEN 'High Accessibility'
            WHEN ROUND(
                SUM(CASE WHEN s.wheelchair_boarding = 1 THEN 1 ELSE 0 END) * 100.0
                / NULLIF(COUNT(DISTINCT s.stop_id), 0), 2
            ) >= 50 THEN 'Medium Accessibility'
            ELSE 'Low Accessibility'
        END AS accessibility_rating
    FROM {{ ref('stg_transit_stops') }}      s
    JOIN {{ ref('stg_transit_stoptimes') }}  st ON s.stop_id  = st.stop_id
    JOIN {{ ref('stg_transit_trips') }}      t  ON st.trip_id = t.trip_id
    JOIN {{ ref('stg_transit_routes') }}     r  ON t.route_id = r.route_id
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
ORDER BY pct_accessible DESCble stops
    -- does this neighbourhood contain
    -- Livability use: highlights if accessible stops are concentrated
    -- in certain neighbourhoods and missing from others
    ROUND(
        accessible_stops * 100.0 /
        NULLIF(SUM(accessible_stops) OVER (), 0), 2
    ) AS pct_of_city_accessible_stops

FROM accessibility_counts
ORDER BY pct_accessible DESC