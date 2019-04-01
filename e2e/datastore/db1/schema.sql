DROP TABLE IF EXISTS events;

CREATE TABLE events (
  id           INT AUTO_INCREMENT PRIMARY KEY,
  event_type   INT,
  quantity     DECIMAL,
  timestamp    TIMESTAMP,
  query_string TEXT NOT NULL
);

