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




DROP TABLE IF EXISTS events2_tmp1;
CREATE TABLE events2_tmp1
(
    id         INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    timestamp  DATETIME,
    event_type INTEGER,
    quantity   DECIMAL(7, 2) DEFAULT NULL,
    modified   TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);


DROP TABLE IF EXISTS events2_tmp2;
CREATE TABLE events2_tmp2
(
    id         INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    timestamp  DATETIME,
    event_type INTEGER,
    quantity   DECIMAL(7, 2) DEFAULT NULL,
    modified   TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);


DROP TABLE IF EXISTS events2_tmp3;
CREATE TABLE events2_tmp3
(
    id         INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    timestamp  DATETIME,
    event_type INTEGER,
    quantity   DECIMAL(7, 2) DEFAULT NULL,
    modified   TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);



DROP TABLE IF EXISTS events2_tmp4;
CREATE TABLE events2_tmp4
(
    id         INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    timestamp  DATETIME,
    event_type INTEGER,
    quantity   DECIMAL(7, 2) DEFAULT NULL,
    modified   TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);



DROP TABLE IF EXISTS events2_tmp5;
CREATE TABLE events2_tmp5
(
    id         INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    timestamp  DATETIME,
    event_type INTEGER,
    quantity   DECIMAL(7, 2) DEFAULT NULL,
    modified   TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);


