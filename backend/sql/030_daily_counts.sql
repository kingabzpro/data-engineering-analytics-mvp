SELECT
    CAST(date_trunc('day', event_time) AS DATE) AS event_date,
    CAST(COUNT(*) AS INTEGER) AS daily_count
FROM fct_events
GROUP BY 1
ORDER BY 1;
