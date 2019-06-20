package com.ranjay.cassandra.models;

import com.datastax.driver.mapping.annotations.Table;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Table(
    name = "songsession",
    keyspace = "events")
public class UserSession{
    private String artist;
    private String song;
    private int itemInSession;
    private String firstName;
    private String lastName;
    private int userId;
    private int sessionId;

    public UserSession(EventData data){
        this.artist = data.getArtist();
        this.song = data.getSong();
        this.itemInSession = data.getItemInSession();
        this.firstName = data.getFirstName();
        this.lastName = data.getLastName();
        this.userId = data.getUserId();
        this.sessionId = data.getSessionId();
    }

}