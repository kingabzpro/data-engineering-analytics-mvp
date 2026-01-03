SELECT
    CAST(COUNT(*) AS INTEGER) AS total_events,
    CAST(COUNT(DISTINCT user_id) AS INTEGER) AS unique_users,
    CAST(COALESCE(SUM(amount), 0) AS DOUBLE) AS total_amount
FROM fct_events;
