CREATE OR REPLACE TABLE fct_events AS
SELECT
    CAST(event_time AS TIMESTAMP) AS event_time,
    CAST(user_id AS INTEGER) AS user_id,
    CAST(event_name AS VARCHAR) AS event_name,
    CAST(category AS VARCHAR) AS category,
    CAST(amount AS DOUBLE) AS amount
FROM raw_events
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY event_time, user_id, event_name, category, amount
    ORDER BY event_time, user_id, event_name, category, amount
) = 1;
