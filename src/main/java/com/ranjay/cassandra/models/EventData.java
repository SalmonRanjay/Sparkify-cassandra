package com.ranjay.cassandra.models;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter

public class EventData{
    private String artist;
    private String auth;
    private String firstName;
    private char gender;
    private int itemInSession;
    private String lastName;
    private double length;
    private String level;
    private String location;
    private String method;
    private String page;
    private String registration;
    private int sessionId;
    private String song;
    private int status;
    private String ts;
    private String userId;

    @Override
    public String toString() {
        return "{" +
            " artist='" + getArtist() + "'" +
            ", auth='" + getAuth() + "'" +
            ", firstName='" + getFirstName() + "'" +
            ", gender='" + getGender() + "'" +
            ", itemInSession='" + getItemInSession() + "'" +
            ", lastName='" + getLastName() + "'" +
            ", length='" + getLength() + "'" +
            ", level='" + getLevel() + "'" +
            ", location='" + getLocation() + "'" +
            ", method='" + getMethod() + "'" +
            ", page='" + getPage() + "'" +
            ", registration='" + getRegistration() + "'" +
            ", sessionId='" + getSessionId() + "'" +
            ", song='" + getSong() + "'" +
            ", status='" + getStatus() + "'" +
            ", ts='" + getTs() + "'" +
            ", userId='" + getUserId() + "'" +
            "}";
    }


     public static class EventBuilder{
        private String artist;
        private String auth;
        private String firstName;
        private char gender;
        private int itemInSession;
        private String lastName;
        private double length;
        private String level;
        private String location;
        private String method;
        private String page;
        private String registration;
        private int sessionId;
        private String song;
        private int status;
        private String ts;
        private String userId;

        public EventBuilder(int sessionId){
            this.sessionId = sessionId;
        }

        public EventBuilder withArtist(String artist){
            this.artist = artist;
            return this;
        }

        public EventBuilder withAuth(String auth){
            this.auth = auth;
            return this;
        }

        public EventBuilder withFirstName(String firstName){
            this.firstName = firstName;
            return this;
        }

        public EventBuilder withGender(char gender){
            this.gender = gender;
            return this;
        }
        
        public EventBuilder withItemInSession(int itemInSession){
            this.itemInSession = itemInSession;
            return this;
        }

        public EventBuilder withLastName(String lastName){
            this.lastName = lastName;
            return this;
        }

        public EventBuilder withLength(double d){
            this.length = length;
            return this;
        }

        public EventBuilder withLevel(String level){
            this.level = level;
            return this;
        }

        public EventBuilder withLocation(String location){
            this.location = location;
            return this;
        }

        public EventBuilder withMethod(String method){
            this.method = method;
            return this;
        }

        public EventBuilder withPage(String page){
            this.page = page;
            return this;
        }

        public EventBuilder withRegistration(String registration){
            this.registration = registration;
            return this;
        }

        public EventBuilder withSong(String song){
            this.song = song;
            return this;
        }

        public EventBuilder withStatus(int status){
            this.status = status;
            return this;
        }

        public EventBuilder withTs(String ts){
            this.ts = ts;
            return this;
        }

        public EventBuilder withUserId(String userId){
            this.userId = userId;
            return this;
        }

        public EventData build(){
            EventData event = new EventData();
            event.artist = this.artist;
            event.auth = this.auth;
            event.firstName = this.firstName;
            event.gender = this.gender;
            event.itemInSession = this.itemInSession;
            event.lastName = this.lastName;
            event.length = this.length;
            event.level = this.level;
            event.location = this.location;
            event.method = this.method;
            event.page = this.page;
            event.registration = this.registration;
            event.sessionId = this.sessionId;
            event.song = this.song;
            event.status = this.status;
            event.ts = this.ts;
            event.userId = this.userId;
            return event;
        }
    }
}