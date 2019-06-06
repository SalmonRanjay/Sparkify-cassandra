package com.ranjay.cassandra;

import java.io.File;

import com.ranjay.cassandra.models.EventData;
import com.ranjay.cassandra.services.CassandraService;
import com.ranjay.cassandra.services.FileService;

/**
 * Hello world!
 *
 */
public class App {
        public static void main(String[] args) {
               
                System.out.println();

               
                FileService.readCSVFile(new File("./data")).forEach(item -> CassandraService.createBoundedStatement
                                .accept(item, "INSERT INTO sessionevents"));
                CassandraService.executeBatchStatment();

                FileService.readCSVFile(new File("./data")).forEach(item -> CassandraService.createBoundedStatement
                                .accept(item, "INSERT INTO usersessions"));
                CassandraService.executeBatchStatment();

                FileService.readCSVFile(new File("./data")).forEach(item -> CassandraService.createBoundedStatement
                                .accept(item, "INSERT INTO songsession"));
                CassandraService.executeBatchStatment();

                // CassandraService.dropTables();
        
                CassandraService.closeResources();

                System.out.println("KEYSPACE: events");
                System.out.println("TABLES: sessionevents, usersessions, songsession");

        }
}
