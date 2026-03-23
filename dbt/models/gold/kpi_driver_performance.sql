{{
    config(
        materialized = 'table',
        schema = 'gold'
    )
}}

WITH trips AS (
    SELECT * FROM {{ ref('trips') }}
),

drivers AS (
    SELECT * FROM pysparkdbt.gold.dimdrivers
    WHERE dbt_valid_to = to_date('9999-12-31')
),

driver_stats AS (
    SELECT
        t.driver_id,
        COUNT(t.trip_id)                               AS total_trips,
        ROUND(SUM(t.fare_amount), 2)                   AS total_earnings,
        ROUND(AVG(t.fare_amount), 2)                   AS avg_fare_per_trip,
        ROUND(SUM(t.distance_km), 2)                   AS total_distance_km,
        ROUND(AVG(t.distance_km), 2)                   AS avg_distance_per_trip,
        ROUND(SUM(t.fare_amount) /
            NULLIF(SUM(t.distance_km), 0), 2)          AS earnings_per_km,
        MIN(DATE(t.trip_start_time))                   AS first_trip_date,
        MAX(DATE(t.trip_start_time))                   AS last_trip_date
    FROM trips t
    GROUP BY t.driver_id
)

SELECT
    d.driver_id,
    d.full_name                                        AS driver_name,
    d.city                                             AS driver_city,
    d.phone_number,

    -- Performance KPIs
    ds.total_trips,
    ds.total_earnings,
    ds.avg_fare_per_trip,
    ds.total_distance_km,
    ds.avg_distance_per_trip,
    ds.earnings_per_km,
    ds.first_trip_date,
    ds.last_trip_date,

    -- Ranking
    RANK() OVER (ORDER BY ds.total_earnings DESC)      AS earnings_rank,
    RANK() OVER (ORDER BY ds.total_trips DESC)         AS trips_rank,

    -- Driver tier
    CASE
        WHEN ds.total_earnings >= 10000 THEN 'Platinum'
        WHEN ds.total_earnings >= 5000  THEN 'Gold'
        WHEN ds.total_earnings >= 1000  THEN 'Silver'
        ELSE 'Bronze'
    END                                                AS driver_tier

FROM driver_stats ds
LEFT JOIN drivers d ON ds.driver_id = d.driver_id
