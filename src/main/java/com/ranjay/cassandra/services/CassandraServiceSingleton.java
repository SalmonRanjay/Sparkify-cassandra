package com.ranjay.cassandra.services;

import java.util.function.Consumer;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.ranjay.cassandra.models.EventData;
import com.ranjay.cassandra.models.SessionEvents;
import com.ranjay.cassandra.models.SongSession;
import com.ranjay.cassandra.models.UserSession;

public class CassandraServiceSingleton {

    private Cluster cluster;
    private Session session;
    private MappingManager manager;
    private Mapper<EventData> mapper;
    private Mapper<SessionEvents> sessionMapper;
    private Mapper<UserSession> userMapper;
    private Mapper<SongSession> songMapper;

    private static CassandraServiceSingleton instance;

    private CassandraServiceSingleton() {
        String serverIP = "127.0.0.1";
        String keyspace = "events";
        cluster = Cluster.builder().addContactPoint(serverIP).withProtocolVersion(ProtocolVersion.V3).build();
        session = cluster.connect();
        createKeySpace();
        useKeySpace();
        initTables();
        manager = new MappingManager(session);
        // mapper = manager.mapper(EventData.class);

        sessionMapper = manager.mapper(SessionEvents.class);
        userMapper = manager.mapper(UserSession.class);
        songMapper = manager.mapper(SongSession.class);
    };

    public static CassandraServiceSingleton getInstance() {
        if (instance == null)
            instance = new CassandraServiceSingleton();

        return instance;
    }

    public void dropTables() {
        session.execute("DROP TABLE IF EXISTS events.sessionEvents");
        System.out.println("events.sessionEvents DELETED");
        session.execute("DROP TABLE IF EXISTS events.userSessions");
        System.out.println("events.userSessions DELETED");
        session.execute("DROP TABLE IF EXISTS events.songSession");
        System.out.println("events.songSession DELETED");
    }

    // create keyspace
    public void createKeySpace() {
        String query = "CREATE KEYSPACE IF NOT EXISTS events WITH replication "
                + "= {'class':'SimpleStrategy', 'replication_factor':1}; ";
        session.execute(query);

    }

      /**
     * Close Cassandra resources
     */
    public void closeResources() {
        cluster.close();
        session.close();
    }

    private void useKeySpace() {
        session.execute(" USE events ");
    }

    // Table Creators
    private void sessionEventsTable() {
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("CREATE TABLE IF NOT EXISTS sessionEvents(")
                .append(" artist TEXT,song TEXT,length double, itemInSession int,").append(" sessionId int,")
                .append(" PRIMARY KEY(itemInSession,sessionId) ").append(") ").append(";");
        session.execute(queryBuilder.toString());
    }

    private void userSessionTable() {
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("CREATE TABLE IF NOT EXISTS userSessions(")
                .append(" artist TEXT,song TEXT,itemInSession int, firstName TEXT,lastName TEXT,")
                .append(" userId int, sessionId int, ")
                .append(" PRIMARY KEY((userId,sessionId),itemInSession,firstName,lastName ) ").append(") ")
                .append("WITH CLUSTERING ORDER BY (itemInSession DESC, firstName ASC, lastName ASC) ").append(";");
        session.execute(queryBuilder.toString());
    }

    private void songSessionTable() {
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("CREATE TABLE IF NOT EXISTS songSession(")
                .append(" firstName TEXT,lastName TEXT, song TEXT,userId int, sessionId int, ").append(" PRIMARY KEY((userId,firstName,lastName )) ")
                .append(") ").append(";");
        session.execute(queryBuilder.toString());
    }

    // Init Tables
    private void initTables() {
        sessionEventsTable();
        userSessionTable();
        songSessionTable();
    };

    // session event mapper
    public Consumer<SessionEvents> mapSessionEventPojoToCQLQuery = (event) -> {
        System.out.println("session event");
        sessionMapper.save(event);
    };

    // user mapper
    public Consumer<UserSession> mapUserEventPojoToCQLQuery = (event) -> {
        System.out.println("User event");
        userMapper.save(event);
    };

    // song mapper
    public Consumer<SongSession> mapSongEventPojoToCQLQuery = (event) -> {
        System.out.println("song event");
        songMapper.save(event);
    };

}CassandraServiceSingleton.java
