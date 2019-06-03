package com.ranjay.cassandra;

import java.io.File;

import com.ranjay.cassandra.services.FileService;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        System.out.println();
        FileService.readCSVFile(new File("./data"))
        .forEach((event) ->{
            System.out.println(event.toString());
        });
    }
}
