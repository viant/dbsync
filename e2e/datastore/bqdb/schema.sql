CREATE OR REPLACE TABLE events(
  id           INT64,
  event_type   INT64,
  quantity     FLOAT64,
  timestamp    TIMESTAMP,
  query_string STRING
);
