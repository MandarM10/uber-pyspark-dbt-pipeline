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

customer_stats AS (
    SELECT
        t.customer_id,
        COUNT(t.trip_id)                                   AS total_trips,
        ROUND(SUM(t.fare_amount), 2)                       AS total_spend,
        ROUND(AVG(t.fare_amount), 2)                       AS avg_spend_per_trip,
        ROUND(SUM(t.distance_km), 2)                       AS total_distance_km,
        MIN(DATE(t.trip_start_time))                       AS first_trip_date,
        MAX(DATE(t.trip_start_time))                       AS last_trip_date,
        DATEDIFF(
            MAX(DATE(t.trip_start_time)),
            MIN(DATE(t.trip_start_time))
        )                                                  AS customer_lifetime_days
    FROM trips t
    GROUP BY t.customer_id
)

SELECT
    c.customer_id,
    c.full_name                                            AS customer_name,
    c.city,
    c.domain                                               AS email_domain,

    -- Engagement KPIs
    cs.total_trips,
    cs.total_spend,
    cs.avg_spend_per_trip,
    cs.total_distance_km,
    cs.first_trip_date,
    cs.last_trip_date,
    cs.customer_lifetime_days,

    -- Revenue per lifetime day
    ROUND(cs.total_spend /
        NULLIF(cs.customer_lifetime_days, 0), 2)          AS revenue_per_lifetime_day,

    -- RFM Segmentation
    CASE
        WHEN cs.total_trips >= 20
             AND cs.total_spend >= 500  THEN 'VIP'
        WHEN cs.total_trips >= 10
             AND cs.total_spend >= 200  THEN 'Loyal'
        WHEN cs.total_trips >= 5        THEN 'Regular'
        WHEN cs.total_trips >= 2        THEN 'Occasional'
        ELSE 'New'
    END                                                    AS customer_segment,

    -- Ranking
    RANK() OVER (ORDER BY cs.total_spend DESC)             AS spend_rank

FROM customer_stats cs
LEFT JOIN customers c ON cs.customer_id = c.customer_id
