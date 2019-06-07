# Sparkify-cassandra
Sparkify Cassandra Project
>Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

They'd like a data engineer to create an Apache Cassandra database which can create queries on song play data to answer the questions, and wish to bring you on the project. Your role is to create a database for this analysis. You'll be able to test your database by running queries given to you by the analytics team from Sparkify to create the results.

## Build

command line:

```sh
mvn clean package
```

## Usage example

Run the jar file.

```sh
java -jar target/sparkifyc-1.0-SNAPSHOT.jar
```

## Tables
CQL Table Names:
* sessionEvents
* userSessions
* songSession

## Select Statements
Create Table:
```
CREATE KEYSPACE events 
WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};
```
Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4
```
Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
SELECT artist, song, length FROM sessionevents WHERE sessionId = 338 and itemInSession = 4;
```

 Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
```
SELECT artist, song FROM usersessions WHERE  userid = 10 AND sessionid = 182;
```
```
SELECT * FROM songsession WHERE song='All Hands Against His Own' ALLOW FILTERING ;
```



## Contributing

1. Fork it (<https://github.com/yourname/yourproject/fork>)
2. Create your feature branch (`git checkout -b feature/fooBar`)
3. Commit your changes (`git commit -am 'Add some fooBar'`)
4. Push to the branch (`git push origin feature/fooBar`)
5. Create a new Pull Request
