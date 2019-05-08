DROP TABLE IF EXISTS events;

CREATE TABLE events
(
  id           INT
  CONSTRAINT events_id PRIMARY KEY,
  event_type   INT,
  quantity     DECIMAL(12, 7) DEFAULT NULL,
  timestamp    TIMESTAMP,
  query_string TEXT
);

