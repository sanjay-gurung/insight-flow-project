CREATE TABLE insight_flow_gold_db.daily_calls_booked_by_source
WITH (
    format = 'PARQUET',
    external_location = 's3://insight-flow-project/gold/daily_calls_booked_by_source/'
) AS

WITH booking AS (
    SELECT
        DATE(from_iso8601_timestamp(event_created_at)) AS booking_date,
        CASE
            WHEN event_type_uri LIKE '%a258c5e3-2e5f-4733-acb9-39cf9f70d537%'
                THEN 'facebook_paid_ads'
            WHEN event_type_uri LIKE '%7fba5121-b4da-4ea6-a06e-e3191c878bb3%'
                THEN 'youtube_paid_ads'
            WHEN event_type_uri LIKE '%ab6e0c06-f684-426f-aa58-c864ab63662f%'
                THEN 'tiktok_paid_ads'
            ELSE 'other'
        END AS source,
        COUNT(DISTINCT scheduled_event_uri) AS calls_booked
    FROM insight_flow_silver_db.calendly_transformed
    WHERE event_created_at IS NOT NULL
    GROUP BY 1, 2
),

dates AS (
    SELECT DISTINCT booking_date
    FROM booking
),

sources AS (
    SELECT 'facebook_paid_ads' AS source
    UNION ALL
    SELECT 'youtube_paid_ads' AS source
    UNION ALL
    SELECT 'tiktok_paid_ads' AS source
    UNION ALL
    SELECT 'other' AS source
),

daily_source AS (
    SELECT
        d.booking_date,
        s.source,
        COALESCE(b.calls_booked, 0) AS calls_booked
    FROM dates d
    CROSS JOIN sources s
    LEFT JOIN booking b
        ON d.booking_date = b.booking_date
        AND s.source = b.source
)

SELECT
    booking_date,

    SUM(
        CASE WHEN source = 'facebook_paid_ads'
        THEN calls_booked ELSE 0 END
    ) AS facebook,

    SUM(
        CASE WHEN source = 'youtube_paid_ads'
        THEN calls_booked ELSE 0 END
    ) AS youtube,

    SUM(
        CASE WHEN source = 'tiktok_paid_ads'
        THEN calls_booked ELSE 0 END
    ) AS tiktok,

    SUM(
        CASE WHEN source = 'other'
        THEN calls_booked ELSE 0 END
    ) AS other

FROM daily_source
GROUP BY booking_date
ORDER BY booking_date;