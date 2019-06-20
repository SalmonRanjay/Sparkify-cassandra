package com.ranjay.cassandra;

import java.io.File;

import com.ranjay.cassandra.models.EventData;
import com.ranjay.cassandra.models.SessionEvents;
import com.ranjay.cassandra.models.SongSession;
import com.ranjay.cassandra.models.UserSession;
import com.ranjay.cassandra.services.CassandraService;
import com.ranjay.cassandra.services.FileService;

/**
 * Hello world!
 *
 */
public class App {

        public static void main(String[] args) {
               
                System.out.println();

               
                // FileService.readCSVFile(new File("./data"))
                // .map((item) -> new SessionEvents(item))
                // .forEach(item -> CassandraService
                //         .mapSessionEventPojoToCQLQuery.accept(item));
                

                FileService.readCSVFile(new File("./data"))
                                .map( (item) -> new UserSession(item))
                                .forEach(item -> CassandraService
                                        .mapUserEventPojoToCQLQuery.accept(item));
                

                // FileService.readCSVFile(new File("./data"))
                //                 .map((item) ->  new SongSession(item))
                //                 .forEach(item -> CassandraService
                //                         .mapSongEventPojoToCQLQuery.accept(item));
                

                // CassandraService.dropTables();
        
                CassandraService.closeResources();

                System.out.println("KEYSPACE: events");
                System.out.println("TABLES: sessionevents, usersessions, songsession");

        }
}
