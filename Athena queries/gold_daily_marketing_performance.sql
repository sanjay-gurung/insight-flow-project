CREATE TABLE insight_flow_gold_db.daily_marketing_performance
WITH (
    format = 'PARQUET',
    external_location = 's3://insight-flow-project/gold/daily_marketing_performance/'
) AS

WITH latest_stats AS (
    SELECT
        media_id,
        stats_play_rate,
        stats_hours_watched,
        stats_engagement
    FROM (
        SELECT
            media_id,
            stats_play_rate,
            stats_hours_watched,
            stats_engagement,
            ROW_NUMBER() OVER (
                PARTITION BY media_id
                ORDER BY snapshot_timestamp DESC
            ) AS rn
        FROM insight_flow_silver_db.wistia_stats_transformed
    )
    WHERE rn = 1
),

media_dedup AS (
    SELECT DISTINCT
        id,
        hashed_id,
        name,
        created
    FROM insight_flow_silver_db.wistia_media_transformed
),

wistia_consolidated AS (
    SELECT
        e.visitor_key,
        e.event_key,
        e.percent_viewed,
        e.received_at,
        m.hashed_id AS media_id,
        m.name AS media_name,
        s.stats_play_rate,
        s.stats_hours_watched,
        s.stats_engagement
    FROM media_dedup m

    JOIN insight_flow_silver_db.wistia_events_transformed e
        ON m.hashed_id = e.media_id

    LEFT JOIN latest_stats s
        ON m.hashed_id = s.media_id
),

wistia_daily AS (
    SELECT
        DATE(from_iso8601_timestamp(received_at)) AS activity_date,
        COUNT(DISTINCT visitor_key) AS unique_video_visitors,
        COUNT(DISTINCT event_key) AS video_events,
        ROUND(AVG(percent_viewed) * 100, 2) AS avg_percent_viewed
    FROM wistia_consolidated
    WHERE received_at IS NOT NULL
    GROUP BY 1
),

crm AS (
    SELECT
        DATE(from_iso8601_timestamp(date_created)) AS activity_date,
        COUNT(DISTINCT lead_id) AS new_leads
    FROM insight_flow_silver_db.crm_leads_transformed
    WHERE date_created IS NOT NULL
    GROUP BY 1
),

calendly AS (
    SELECT
        DATE(from_iso8601_timestamp(event_created_at)) AS activity_date,
        COUNT(DISTINCT scheduled_event_uri) AS total_bookings
    FROM insight_flow_silver_db.calendly_transformed
    WHERE event_created_at IS NOT NULL
    GROUP BY 1
),

marketing AS (
    SELECT
        CAST(date AS DATE) AS activity_date,
        ROUND(SUM(spend), 2) AS total_marketing_spend
    FROM insight_flow_silver_db.marketing_spend_transformed
    GROUP BY 1
),

dates AS (
    SELECT activity_date FROM crm
    UNION
    SELECT activity_date FROM wistia_daily
    UNION
    SELECT activity_date FROM calendly
    UNION
    SELECT activity_date FROM marketing
)

SELECT
    d.activity_date,
    m.total_marketing_spend,
    w.unique_video_visitors,
    w.video_events,
    w.avg_percent_viewed,
    c.total_bookings,
    r.new_leads
FROM dates d

LEFT JOIN marketing m
    ON d.activity_date = m.activity_date

LEFT JOIN wistia_daily w
    ON d.activity_date = w.activity_date

LEFT JOIN calendly c
    ON d.activity_date = c.activity_date

LEFT JOIN crm r
    ON d.activity_date = r.activity_date

WHERE d.activity_date >= DATE '2026-10-06';