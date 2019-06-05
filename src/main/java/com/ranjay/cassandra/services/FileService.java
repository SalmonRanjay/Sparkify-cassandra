package com.ranjay.cassandra.services;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import com.ranjay.cassandra.models.EventData;
import com.ranjay.cassandra.models.EventData.EventBuilder;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;

public final class FileService {
    private FileService() {
    };

   

    public static Stream<EventData> readCSVFile(File rootDir) {
        return FileUtils.listFiles(rootDir, null, true)
                .stream()
                .map( t -> extractData.apply(t))
                .flatMap(event -> event.stream());
           
    }

    private static Function<File, List<EventData>> extractData = (t) ->{
        List<EventData> events = new  ArrayList<>();
        try (Reader in = new FileReader(t);) {
            Iterable<CSVRecord> records = CSVFormat.EXCEL.withHeader().parse(in);
            for (CSVRecord record : records) {
                EventData event = new EventBuilder(Integer.parseInt(record.get("sessionId")))
                    .withArtist(record.get("artist"))
                    .withAuth(record.get("auth"))
                    .withFirstName(record.get("firstName"))
                    .withItemInSession(Integer.parseInt(
                                !record.get("itemInSession").isEmpty() ? record.get("itemInSession") : "0"))
                    .withGender(
                        !record.get("gender").isEmpty() ? record.get("gender") : "-"
                    )
                    .withLastName(record.get("lastName"))
                    .withLength(Double.parseDouble(
                                !record.get("length").isEmpty() ? record.get("length") : "0"))
                    .withLocation(record.get("location"))
                    .withMethod(record.get("method"))
                    .withPage(record.get("page"))
                    .withRegistration(record.get("registration"))
                    .withSong(record.get("song"))
                    .withStatus(Integer.parseInt(
                                !record.get("status").isEmpty() ? record.get("status") : "0"
                    ))
                    .withTs(record.get("ts"))
                    .withUserId(record.get("userId"))
                    .build();
                    events.add(event);   
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return events;
    };
}