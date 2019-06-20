package com.ranjay.cassandra.models;

import com.datastax.driver.mapping.annotations.Table;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Table(
    name = "sessionevents",
    keyspace = "events")
public class SessionEvents{
    private String artist;
    private int itemInSession;
    private String song;
    private double length;
    private int sessionId;


    public SessionEvents(EventData data){
        this.artist = data.getArtist();
        this.itemInSession = data.getItemInSession();
        this.song = data.getSong();
        this.length = data.getLength();
        this.sessionId = data.getSessionId();
    }

}