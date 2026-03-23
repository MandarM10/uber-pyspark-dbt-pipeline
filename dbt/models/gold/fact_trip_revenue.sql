{{
    config(
        materialized = 'table',
        schema = 'gold'
    )
}}

WITH trips AS (
    SELECT * FROM {{ ref('trips') }}
),

customers AS (
    SELECT * FROM pysparkdbt.gold.dimcustomers
    WHERE dbt_valid_to = to_date('9999-12-31')
),

drivers AS (
    SELECT * FROM pysparkdbt.gold.dimdrivers
    WHERE dbt_valid_to = to_date('9999-12-31')
),

locations AS (
    SELECT * FROM pysparkdbt.gold.dimlocations
    WHERE dbt_valid_to = to_date('9999-12-31')
),

payments AS (
    SELECT * FROM pysparkdbt.gold.dimpayments
    WHERE dbt_valid_to = to_date('9999-12-31')
)

SELECT
    -- Trip identifiers
    t.trip_id,
    t.trip_start_time,
    t.trip_end_time,

    -- Dimensions
    d.full_name                                           AS driver_name,
    c.full_name                                           AS customer_name,
    c.city                                                AS customer_city,
    l.city                                                AS pickup_city,

    -- Trip metrics
    t.distance_km,
    t.fare_amount,
    p.online_payment_status,

    -- Derived KPIs
    ROUND(t.fare_amount / NULLIF(t.distance_km, 0), 2)   AS revenue_per_km,
    DATEDIFF(MINUTE, t.trip_start_time, t.trip_end_time) AS trip_duration_minutes,

    -- Date dimensions
    DATE(t.trip_start_time)                               AS trip_date,
    MONTH(t.trip_start_time)                              AS trip_month,
    YEAR(t.trip_start_time)                               AS trip_year,
    DAYOFWEEK(t.trip_start_time)                          AS day_of_week

FROM trips t
LEFT JOIN drivers   d ON t.driver_id  = d.driver_id
LEFT JOIN customers c ON t.customer_id = c.customer_id
LEFT JOIN locations l ON t.vehicle_id  = l.location_id
LEFT JOIN payments  p ON t.trip_id     = p.trip_id
