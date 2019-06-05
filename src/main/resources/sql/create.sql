CREATE KEYSPACE events 
WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};

CREATE TABLE IF NOT EXISTS sessionEvents(
    artist TEXT,
    auth TEXT,
    firstName TEXT,
    gender TEXT,
    itemInSession int,
    lastName TEXT,
    length double,
    level TEXT,
    location TEXT,
    method TEXT,
    page TEXT,
    registration TEXT,
    sessionId int,
    song TEXT,
    status INTEGER,
    ts TEXT,
    userId TEXT,
    PRIMARY KEY(itemInSession,sessionId)
);