DROP TABLE IF EXISTS events;
CREATE TABLE events
(
  id           INT AUTO_INCREMENT PRIMARY KEY,
  event_type   INT,
  quantity     DECIMAL(10,7),
  timestamp    TIMESTAMP,
  query_string VARCHAR(255)
);


DROP TABLE IF EXISTS nevents;
CREATE TABLE nevents
(
  id           INT,
  event_type   INT,
  quantity     DECIMAL(10,7),
  timestamp    TIMESTAMP,
  query_string TEXT
);


