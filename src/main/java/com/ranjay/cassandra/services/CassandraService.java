package com.ranjay.cassandra.services;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.ranjay.cassandra.models.EventData;

public final class CassandraService {

    private static Cluster cluster;
    private static Session session;
    private static MappingManager manager;

    static{
        System.out.println("Private Constructor called");
        manager = new MappingManager(session);
    }

    private CassandraService() {
        
    };

    private static Session connect() {   
        // Connect to the cluster and keyspace "demo"
        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect("demo");
        return session;
    }

    public static void loadTable1(EventData event){
       
        // Mapper<EventData> mapper = manager.mapper(EventData.class);
        // mapper.save(event);
    }


}