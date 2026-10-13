CREATE TABLE insight_flow_gold_db.cost_per_booking_by_channel
WITH (
    format = 'PARQUET',
    external_location = 's3://insight-flow-project/gold/cost_per_booking_by_channel/'
) AS

WITH spend AS (
    SELECT
        channel,
        SUM(spend) AS total_spend
    FROM insight_flow_silver_db.marketing_spend_transformed
    GROUP BY 1
),
events AS (
    SELECT
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
    GROUP BY 1
)
SELECT
    s.channel,
    s.total_spend,
    e.calls_booked,
    s.total_spend / NULLIF(e.calls_booked, 0) AS cost_per_booking
FROM spend s
JOIN events e
    ON s.channel = e.source;