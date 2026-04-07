SELECT
    stop_id,
    stop_code,
    stop_name,
    stop_lat,
    stop_lon,
    wheelchair_boarding,
    zone_id,
    location_type,
    CASE
        -- Downtown
        WHEN stop_lat BETWEEN 49.279 AND 49.290
             AND stop_lon BETWEEN -123.135 AND -123.110
             THEN 'Downtown'

        -- West End
        WHEN stop_lat BETWEEN 49.279 AND 49.295
             AND stop_lon BETWEEN -123.155 AND -123.135
             THEN 'West End'

        -- Strathcona
        WHEN stop_lat BETWEEN 49.270 AND 49.285
             AND stop_lon BETWEEN -123.095 AND -123.070
             THEN 'Strathcona'

        -- Grandview-Woodland
        WHEN stop_lat BETWEEN 49.265 AND 49.282
             AND stop_lon BETWEEN -123.075 AND -123.050
             THEN 'Grandview-Woodland'

        -- Hastings-Sunrise
        WHEN stop_lat BETWEEN 49.270 AND 49.290
             AND stop_lon BETWEEN -123.055 AND -123.020
             THEN 'Hastings-Sunrise'

        -- West Point Grey
        WHEN stop_lat BETWEEN 49.265 AND 49.285
             AND stop_lon BETWEEN -123.220 AND -123.175
             THEN 'West Point Grey'

        -- Kitsilano
        WHEN stop_lat BETWEEN 49.260 AND 49.279
             AND stop_lon BETWEEN -123.175 AND -123.140
             THEN 'Kitsilano'

        -- Fairview
        WHEN stop_lat BETWEEN 49.260 AND 49.279
             AND stop_lon BETWEEN -123.140 AND -123.115
             THEN 'Fairview'

        -- Mount Pleasant
        WHEN stop_lat BETWEEN 49.255 AND 49.275
             AND stop_lon BETWEEN -123.115 AND -123.085
             THEN 'Mount Pleasant'

        -- Dunbar-Southlands
        WHEN stop_lat BETWEEN 49.235 AND 49.265
             AND stop_lon BETWEEN -123.220 AND -123.175
             THEN 'Dunbar-Southlands'

        -- Arbutus-Ridge
        WHEN stop_lat BETWEEN 49.240 AND 49.265
             AND stop_lon BETWEEN -123.175 AND -123.150
             THEN 'Arbutus-Ridge'

        -- Shaughnessy
        WHEN stop_lat BETWEEN 49.240 AND 49.265
             AND stop_lon BETWEEN -123.150 AND -123.125
             THEN 'Shaughnessy'

        -- South Cambie
        WHEN stop_lat BETWEEN 49.240 AND 49.265
             AND stop_lon BETWEEN -123.125 AND -123.110
             THEN 'South Cambie'

        -- Riley Park
        WHEN stop_lat BETWEEN 49.240 AND 49.262
             AND stop_lon BETWEEN -123.110 AND -123.090
             THEN 'Riley Park'

        -- Kensington-Cedar Cottage
        WHEN stop_lat BETWEEN 49.240 AND 49.262
             AND stop_lon BETWEEN -123.090 AND -123.060
             THEN 'Kensington-Cedar Cottage'

        -- Renfrew-Collingwood
        WHEN stop_lat BETWEEN 49.240 AND 49.270
             AND stop_lon BETWEEN -123.060 AND -123.030
             THEN 'Renfrew-Collingwood'

        -- Kerrisdale
        WHEN stop_lat BETWEEN 49.220 AND 49.245
             AND stop_lon BETWEEN -123.175 AND -123.145
             THEN 'Kerrisdale'

        -- Oakridge
        WHEN stop_lat BETWEEN 49.220 AND 49.245
             AND stop_lon BETWEEN -123.145 AND -123.115
             THEN 'Oakridge'

        -- Marpole
        WHEN stop_lat BETWEEN 49.200 AND 49.225
             AND stop_lon BETWEEN -123.145 AND -123.100
             THEN 'Marpole'

        -- Sunset
        WHEN stop_lat BETWEEN 49.220 AND 49.245
             AND stop_lon BETWEEN -123.100 AND -123.060
             THEN 'Sunset'

        -- Victoria-Fraserview
        WHEN stop_lat BETWEEN 49.200 AND 49.228
             AND stop_lon BETWEEN -123.060 AND -123.025
             THEN 'Victoria-Fraserview'

        -- Killarney
        WHEN stop_lat BETWEEN 49.200 AND 49.240
             AND stop_lon BETWEEN -123.025 AND -122.990
             THEN 'Killarney'

        ELSE 'Outside Vancouver'
    END AS neighbourhood

FROM VANCOUVER_DATA.SILVER.TRANSIT_STOPS_CLEAN