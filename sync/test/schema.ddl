DROP TABLE IF EXISTS events;
CREATE TABLE events (
  id         INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  ts         DATETIME,
  event_type INTEGER,
  ua         VARCHAR(255) DEFAULT NULL,
  dnt        TINYINT(1)   DEFAULT '0',
  charge     DECIMAL(7,2) DEFAULT NULL,
  payment    DECIMAL(7,2) DEFAULT NULL,
  modified   TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS events_clone;
CREATE TABLE events_clone (
    id         INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    ts         DATETIME,
    event_type INTEGER,
    ua         VARCHAR(255) DEFAULT NULL,
    dnt        TINYINT(1)   DEFAULT '0',
    charge     DECIMAL(7,2) DEFAULT NULL,
    payment    DECIMAL(7,2) DEFAULT NULL,
    modified   TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
);
