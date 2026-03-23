{{
    config(
        materialized = 'table',
        schema = 'gold'
    )
}}

WITH trips AS (
    SELECT * FROM {{ ref('trips') }}
),

payments AS (
    SELECT * FROM pysparkdbt.gold.dimpayments
    WHERE dbt_valid_to = to_date('9999-12-31')
),

monthly_revenue AS (
    SELECT
        YEAR(t.trip_start_time)                            AS trip_year,
        MONTH(t.trip_start_time)                           AS trip_month,
        p.payment_method,
        p.online_payment_status,

        -- Volume KPIs
        COUNT(t.trip_id)                                   AS total_trips,
        COUNT(DISTINCT t.driver_id)                        AS active_drivers,
        COUNT(DISTINCT t.customer_id)                      AS active_customers,

        -- Revenue KPIs
        ROUND(SUM(t.fare_amount), 2)                       AS total_revenue,
        ROUND(AVG(t.fare_amount), 2)                       AS avg_fare,
        ROUND(MAX(t.fare_amount), 2)                       AS max_fare,
        ROUND(MIN(t.fare_amount), 2)                       AS min_fare,

        -- Distance KPIs
        ROUND(SUM(t.distance_km), 2)                       AS total_distance_km,
        ROUND(AVG(t.distance_km), 2)                       AS avg_distance_km

    FROM trips t
    LEFT JOIN payments p ON t.trip_id = p.trip_id
    GROUP BY
        YEAR(t.trip_start_time),
        MONTH(t.trip_start_time),
        p.payment_method,
        p.online_payment_status
)

SELECT
    *,
    -- Month over Month revenue growth
    ROUND(
        (total_revenue - LAG(total_revenue) OVER (
            PARTITION BY payment_method
            ORDER BY trip_year, trip_month
        )) / NULLIF(LAG(total_revenue) OVER (
            PARTITION BY payment_method
            ORDER BY trip_year, trip_month
        ), 0) * 100, 2
    )                                                      AS mom_revenue_growth_pct

FROM monthly_revenue
ORDER BY trip_year, trip_month
