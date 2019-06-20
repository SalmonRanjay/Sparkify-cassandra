package com.ranjay.cassandra.services;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.ranjay.cassandra.models.EventData;
import com.ranjay.cassandra.models.SessionEvents;
import com.ranjay.cassandra.models.SongSession;
import com.ranjay.cassandra.models.UserSession;

import org.apache.commons.collections4.ListUtils;

import io.netty.util.internal.shaded.org.jctools.queues.MessagePassingQueue.Consumer;

public final class CassandraService {
    private static Cluster cluster;
    private static Session session;
    private static MappingManager manager;
    private static Mapper<EventData> mapper;
    private static Mapper<SessionEvents> sessionMapper;
    private static Mapper<UserSession> userMapper;
    private static Mapper<SongSession> songMapper;
    private static List<BoundStatement> boundList = new ArrayList<>();

    static {

        String serverIP = "127.0.0.1";
        String keyspace = "events";
        cluster = Cluster.builder().addContactPoint(serverIP).withProtocolVersion(ProtocolVersion.V3).build();
        session = cluster.connect();
        createKeySpace();
        useKeySpace();
        initTables();
        manager = new MappingManager(session);
        // mapper = manager.mapper(EventData.class);
        // sessionMapper = manager.mapper(SessionEvents.class);
        userMapper = new MappingManager(session).mapper(UserSession.class);
        // songMapper = new MappingManager(session).mapper(SongSession.class);
    }

    private CassandraService() {
    };

    public static BiConsumer<EventData, String> createBoundedStatement = (event, cqlInsert) -> {
        boundList.add(event.createBoundStatement(createPreparedStatement(cqlInsert)));
    };

    /**
     * Create microbatches of size 100 from a List of BoundStatements lists
     */
    public static void executeBatchStatment() {
        List<List<BoundStatement>> output = ListUtils.partition(boundList, 100);
        for (List<BoundStatement> boundedStatement : output) {
            BatchStatement microBatches = new BatchStatement();
            microBatches.addAll(boundedStatement);
            if (session.execute(microBatches) != null)
                System.out.println("Successfully Inserted Batch of size: " + microBatches.size());

        }
    }

    public static void createKeySpace() {
        String query = "CREATE KEYSPACE IF NOT EXISTS events WITH replication "
                + "= {'class':'SimpleStrategy', 'replication_factor':1}; ";
        session.execute(query);

    }

    /**
     * Creates a cassandara prepared statement using stringBuilder.
     * 
     * @param insertText insert statement to pass to the string builder function
     * 
     * @return Returns a prepared statement
     */
    private static PreparedStatement createPreparedStatement(String insertText) {

        StringBuilder sqlStatement = new StringBuilder();
        sqlStatement.append(insertText).append(" (artist,auth,firstName,").append("gender,itemInSession,lastName,")
                .append("length,level,location,").append("method,page,registration,").append("sessionId,song,status,")
                .append("ts,userId)").append("values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);");
        return session.prepare(sqlStatement.toString());
    }

    /**
     * Close Cassandra resources
     */
    public static void closeResources() {
        cluster.close();
        session.close();
    }

    /**
     * Creates Database Tables
     * 
     * @param createStatement String representing Table Name
     * @param primaryKey      Strring Representing Primary key consisiting of
     *                        partition key and clustering key
     */
    private static void createTable(String createStatement, String primaryKey, String clusteringOrder) {
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append(createStatement);
        queryBuilder.append(" artist TEXT,auth TEXT, firstName TEXT,gender TEXT, itemInSession int,");
        queryBuilder.append("lastName TEXT,length double,level TEXT,location TEXT,method TEXT,");
        queryBuilder.append("page TEXT, registration TEXT, sessionId int, song TEXT, status int, ts TEXT, userId int,");
        queryBuilder.append(primaryKey);
        queryBuilder.append(") ");
        queryBuilder.append(clusteringOrder);
        queryBuilder.append(";");
        session.execute(queryBuilder.toString());
    }


    public static void dropTables() {
        session.execute("DROP TABLE IF EXISTS events.sessionEvents");
        System.out.println("events.sessionEvents DELETED");
        session.execute("DROP TABLE IF EXISTS events.userSessions");
        System.out.println("events.userSessions DELETED");
        session.execute("DROP TABLE IF EXISTS events.songSession");
        System.out.println("events.songSession DELETED");
    }

    public static Consumer<EventData> mapEventPojoToCQLQuery = (event) -> {
        mapper.save(event);
    };


    private static void useKeySpace() {
        session.execute(" USE events ");
    }

    // Table Creators
    private static void sessionEventsTable() {
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder
        .append( "CREATE TABLE IF NOT EXISTS sessionEvents(")
        .append(" artist TEXT,song TEXT,length double, itemInSession int,")
        .append(" sessionId int,")
        .append( " PRIMARY KEY(itemInSession,sessionId) ")
        .append(") ")
        .append(";");
        session.execute(queryBuilder.toString());
    }

    private static void userSessionTable() {
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("CREATE TABLE IF NOT EXISTS userSessions(")
        .append(" artist TEXT,song TEXT,itemInSession int, firstName TEXT,lastName TEXT,")
        .append( " userId int, sessionId int, ")
        .append(" PRIMARY KEY((userId,sessionId),itemInSession,firstName,lastName ) ")
        .append(") ")
        .append("WITH CLUSTERING ORDER BY (itemInSession DESC, firstName ASC, lastName ASC) ")
        .append(";");
        session.execute(queryBuilder.toString());
    }

    private static void songSessionTable() {
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("CREATE TABLE IF NOT EXISTS songSession(")
        .append(" firstName TEXT,lastName TEXT,page TEXT, song TEXT, ")
        .append(" PRIMARY KEY(song, page) ")
        .append(") ")
        .append(";");
        session.execute(queryBuilder.toString());
    }

    // Init Tables
    private static void initTables() {
        // String sessionTable = "CREATE TABLE IF NOT EXISTS sessionEvents(";
        // String sessionKey = " PRIMARY KEY(itemInSession,sessionId) ";

        // String userSession = "CREATE TABLE IF NOT EXISTS userSessions(";
        // String userKey = " PRIMARY KEY((userId,sessionId),itemInSession,firstName,lastName ) ";
        // String clusteringOrder = "WITH CLUSTERING ORDER BY (itemInSession DESC, firstName ASC, lastName ASC) ";

        // String songSession = "CREATE TABLE IF NOT EXISTS songSession(";
        // String songKey = " PRIMARY KEY(sessionId,song) ";
        sessionEventsTable();
        userSessionTable();
        songSessionTable();
    };

    // session event mapper
    public static Consumer<SessionEvents> mapSessionEventPojoToCQLQuery = (event) -> {
        sessionMapper.save(event);
    };

    // user mapper
    public static Consumer<UserSession> mapUserEventPojoToCQLQuery = (event) -> {
        userMapper.save(event);
    };

    // song mapper
    public static Consumer<SongSession> mapSongEventPojoToCQLQuery = (event) -> {
        songMapper.save(event);
    };
}