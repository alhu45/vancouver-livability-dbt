WITH service_counts AS (
    SELECT
        s.neighbourhood,
        r."route_short_name",
        r."route_long_name",
        r."route_type",

        COUNT(DISTINCT t."trip_id") AS total_trips,

        SUM(CASE WHEN c."monday"    = 1 THEN 1 ELSE 0 END) AS monday_trips,
        SUM(CASE WHEN c."tuesday"   = 1 THEN 1 ELSE 0 END) AS tuesday_trips,
        SUM(CASE WHEN c."wednesday" = 1 THEN 1 ELSE 0 END) AS wednesday_trips,
        SUM(CASE WHEN c."thursday"  = 1 THEN 1 ELSE 0 END) AS thursday_trips,
        SUM(CASE WHEN c."friday"    = 1 THEN 1 ELSE 0 END) AS friday_trips,
        SUM(CASE WHEN c."saturday"  = 1 THEN 1 ELSE 0 END) AS saturday_trips,
        SUM(CASE WHEN c."sunday"    = 1 THEN 1 ELSE 0 END) AS sunday_trips,

        SUM(CASE WHEN c."monday" + c."tuesday" + c."wednesday" +
                 c."thursday" + c."friday" > 0 THEN 1 ELSE 0 END) AS total_weekday_trips,

        SUM(CASE WHEN c."saturday" + c."sunday" > 0
                 THEN 1 ELSE 0 END)                              AS total_weekend_trips,

        ROUND(
            SUM(CASE WHEN c."saturday" + c."sunday" > 0 THEN 1 ELSE 0 END) * 100.0
            / NULLIF(COUNT(DISTINCT t."trip_id"), 0), 2
        ) AS pct_weekend_coverage

    FROM {{ ref('stg_transit_trips') }}      t
    JOIN {{ ref('stg_transit_routes') }}     r  ON t."route_id"   = r."route_id"
    JOIN {{ ref('stg_transit_calendar') }}   c  ON t."service_id" = c."service_id"
    JOIN {{ ref('stg_transit_stoptimes') }}  st ON t."trip_id"    = st."trip_id"
    JOIN {{ ref('stg_transit_stops') }}      s  ON st."stop_id"   = s."stop_id"

    WHERE s.neighbourhood != 'Outside Vancouver'

    GROUP BY 1, 2, 3, 4
)

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
    RANK() OVER (ORDER BY pct_weekend_coverage DESC)                                    AS weekend_coverage_rank,
    ROUND(total_weekend_trips - AVG(total_weekend_trips) OVER (), 2)                    AS vs_city_avg_weekend_trips,
    RANK() OVER (ORDER BY total_weekday_trips DESC)                                     AS weekday_rank,
    ROUND(
        (total_weekday_trips - total_weekend_trips) -
        AVG(total_weekday_trips - total_weekend_trips) OVER (), 2
    )                                                                                   AS weekend_gap_vs_city_avg

FROM service_counts
ORDER BY neighbourhood, total_trips DESC