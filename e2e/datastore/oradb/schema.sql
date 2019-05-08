BEGIN
EXECUTE IMMEDIATE 'DROP TABLE events';
EXCEPTION
WHEN OTHERS THEN
IF SQLCODE != -942 THEN
RAISE;
END IF;
END;

CREATE TABLE events (
   id               NUMBER(5) PRIMARY KEY,
   event_type        NUMBER(5),
   quantity         DECIMAL(12, 7),
   query_string     VARCHAR2(255),
   timestamp        timestamp(0)
);


