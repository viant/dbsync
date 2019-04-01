CREATE OR REPLACE TABLE events (
  id         INT64 NOT NULL,
  event_type INT64 NOT NULL,
  quantity   FLOAT64,
  timestamp   TIMESTAMP,
  query_string STRING
);