CREATE TABLE events_clone_tmp(
  id         INTEGER PRIMARY KEY,
  ts         DATETIME,
  event_type INTEGER,
  ua         VARCHAR(255),
  dnt        TINYINT(1),
  charge     DECIMAL(7,2),
  payment    DECIMAL(7,2),
  modified   TIMESTAMP
);