SELECT artist, song, length FROM sessionevents WHERE sessionId = 338 and itemInSession = 4;

SELECT artist, song FROM usersessions WHERE  userid = 10 AND sessionid = 182;

SELECT * FROM songsession WHERE song='All Hands Against His Own' ALLOW FILTERING ;