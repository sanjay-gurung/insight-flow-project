CREATE TABLE insight_flow_gold_db.booking_trend_over_time
WITH (
    format = 'PARQUET',
    external_location = 's3://insight-flow-project/gold/booking_trends_over_time/'
) AS

SELECT
    DATE(from_iso8601_timestamp(event_created_at)) AS booking_date,
    COUNT(DISTINCT scheduled_event_uri) AS calls_booked,
    CASE
        WHEN event_type_uri LIKE '%a258c5e3-2e5f-4733-acb9-39cf9f70d537%' THEN 'facebook_paid_ads'
        WHEN event_type_uri LIKE '%7fba5121-b4da-4ea6-a06e-e3191c878bb3%' THEN 'youtube_paid_ads'
        WHEN event_type_uri LIKE '%ab6e0c06-f684-426f-aa58-c864ab63662f%' THEN 'tiktok_paid_ads'
        ELSE 'other'
    END AS channel
FROM insight_flow_silver_db.calendly_transformed
GROUP BY 1, 3
ORDER BY 1;