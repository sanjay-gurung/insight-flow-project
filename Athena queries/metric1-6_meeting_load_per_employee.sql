CREATE TABLE insight_flow_gold_db.meeting_load_per_employee
WITH (
    format = 'PARQUET',
    external_location = 's3://insight-flow-project/gold/meeting_load_per_employee/'
) AS

WITH weekly_meetings AS (
    SELECT
        employee_name,
        week(from_iso8601_timestamp(event_start_time)) AS week_number,
        COUNT(DISTINCT scheduled_event_uri) AS number_of_meetings
    FROM insight_flow_silver_db.calendly_transformed
    GROUP BY 1, 2
)

SELECT
    employee_name,
    SUM(number_of_meetings) AS total_meetings,
    ROUND(SUM(number_of_meetings)  / COUNT(DISTINCT week_number), 2) AS avg_meetings_per_week
FROM weekly_meetings
GROUP BY employee_name
ORDER BY avg_meetings_per_week DESC;
