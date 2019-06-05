package com.ranjay.cassandra.services;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.QueryTrace.Event;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.ranjay.cassandra.models.EventData;

import io.netty.util.internal.shaded.org.jctools.queues.MessagePassingQueue.Consumer;

public final class CassandraService {

    private static Cluster cluster;
    private static Session session;
    private static MappingManager manager;
    private static BatchStatement batchStatement;
    private static Mapper<EventData> mapper;

    static {
        System.out.println("Private Constructor called");
        String serverIP = "127.0.0.1";
        String keyspace = "events";
        cluster = Cluster.builder().addContactPoint(serverIP).withProtocolVersion(ProtocolVersion.V3).build();
        session = cluster.connect();
        // createKeySpace();
        useKeySpace();
        createTable(" PRIMARY KEY(itemInSession,sessionId) ");
        manager = new MappingManager(session);
        mapper = manager.mapper(EventData.class);
        
    }

    private CassandraService() {

    };
    public static Consumer<EventData> createBoundStatement = (event) -> {
        mapper.save(event);
    };

    public static void createKeySpace() {
        String query = "CREATE KEYSPACE events WITH replication "
                + "= {'class':'SimpleStrategy', 'replication_factor':1}; ";
        session.execute(query);
        
    }
    private static void createPreparedStatement(){
         // batchStatement = new BatchStatement();
        // String sqlStatement = "INSERT INTO sessionQuery "
        // +"(artist,auth,firstName,"
        // +"gender,itemInSession,lastName,"
        // +"length,level,location,"
        // +"method,page,registration,"
        // +"sessionId,song,status,"
        // +"ts,userId)"
        // +"values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
    }
    public static void closeResources() {
        cluster.close();
        session.close();
    }

    private static void createTable(String primaryKey){
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("CREATE TABLE IF NOT EXISTS sessionEvents(");
        queryBuilder.append(" artist TEXT,auth TEXT, firstName TEXT,gender TEXT, itemInSession int,");
        queryBuilder.append("lastName TEXT,length double,level TEXT,location TEXT,method TEXT,");
        queryBuilder.append("page TEXT, registration TEXT, sessionId int, song TEXT, status int, ts TEXT, userId TEXT,");
        queryBuilder.append(primaryKey);
        queryBuilder.append(");");
        session.execute(queryBuilder.toString());
    }
    private static void useKeySpace(){
        session.execute(" USE events ");
    }


}