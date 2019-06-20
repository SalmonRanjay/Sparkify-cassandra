package com.ranjay.cassandra.models;

import com.datastax.driver.mapping.annotations.Table;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Table(
    name = "usersessions",
    keyspace = "events")
public class SongSession{
    private String firstName;
    private String lastName;
    private String page;
    private String song;

    public SongSession(EventData data){
        this.firstName = data.getFirstName();
        this.lastName = data.getLastName();
        this.page = data.getPage();
        this.song = data.getSong();
    }
}