DROP TABLE IF EXISTS events1;
CREATE TABLE events1
(
    id         INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    timestamp  DATETIME,
    event_type INTEGER,
    quantity   DECIMAL(7, 2) DEFAULT NULL,
    modified   TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS events2;
CREATE TABLE events2
(
    id         INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    timestamp  DATETIME,
    event_type INTEGER,
    quantity   DECIMAL(7, 2) DEFAULT NULL,
    modified   TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);


DROP TABLE IF EXISTS events2_tmp;
CREATE TABLE events2_tmp
(
    id         INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    timestamp  DATETIME,
    event_type INTEGER,
    quantity   DECIMAL(7, 2) DEFAULT NULL,
    modified   TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);



DROP TABLE IF EXISTS events2_tmp_chunk_00000;
CREATE TABLE events2_tmp_chunk_00000
(
    id         INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    timestamp  DATETIME,
    event_type INTEGER,
    quantity   DECIMAL(7, 2) DEFAULT NULL,
    modified   TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);


DROP TABLE IF EXISTS events2_tmp_chunk_00001;
CREATE TABLE events2_tmp_chunk_00001
(
    id         INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    timestamp  DATETIME,
    event_type INTEGER,
    quantity   DECIMAL(7, 2) DEFAULT NULL,
    modified   TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);



DROP TABLE IF EXISTS events2_tmp_chunk_00002;
CREATE TABLE events2_tmp_chunk_00002
(
    id         INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    timestamp  DATETIME,
    event_type INTEGER,
    quantity   DECIMAL(7, 2) DEFAULT NULL,
    modified   TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);

