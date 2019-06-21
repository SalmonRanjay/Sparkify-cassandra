package com.ranjay.cassandra;

import java.io.File;

import com.ranjay.cassandra.models.EventData;
import com.ranjay.cassandra.models.SessionEvents;
import com.ranjay.cassandra.models.SongSession;
import com.ranjay.cassandra.models.UserSession;
import com.ranjay.cassandra.services.CassandraService;
import com.ranjay.cassandra.services.CassandraServiceSingleton;
import com.ranjay.cassandra.services.FileService;

/**
 * Hello world!
 *
 */
public class App {

        public static void main(String[] args) {

                CassandraServiceSingleton cassandraService = CassandraServiceSingleton.getInstance();

                // FileService.readCSVFile(new File("./data")).map((eventData) -> new SessionEvents(eventData))
                //                 .forEach(item -> cassandraService.mapSessionEventPojoToCQLQuery.accept(item));

                // FileService.readCSVFile(new File("./data")).map((eventData) -> new UserSession(eventData))
                //                 .forEach(item -> cassandraService.mapUserEventPojoToCQLQuery.accept(item));

                // FileService.readCSVFile(new File("./data")).map((eventData) -> new SongSession(eventData))
                //                 .forEach(item -> cassandraService.mapSongEventPojoToCQLQuery.accept(item));

                cassandraService.dropTables();

                cassandraService.closeResources();

                System.out.println("KEYSPACE: events");
                System.out.println("TABLES: sessionevents, usersessions, songsession");

        }
}
