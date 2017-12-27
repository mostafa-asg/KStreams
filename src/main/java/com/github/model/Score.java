package com.github.model;

import java.util.Date;

/**
 * Model to represent a user score
 *
 */
public class Score implements Comparable<Score> {

    //Username or userID
    private String user;
    private int score;
    private String date;

    public Score(){

    }

    public Score(String user, int score , long date) {
        this.user = user;
        this.score = score;
        setDate(date);
    }

    public String getDate() {
        return date;
    }

    public void setDate(long date) {
        this.date = new Date(date).toString();
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public int getScore() {
        return score;
    }

    public void setScore(int score) {
        this.score = score;
    }

    public String toJson(){
        return String.format("{ \"time\" : \"%s\" , \"user\" : \"%s\" , \"score\" : %d }" , this.getDate() , this.getUser() , this.getScore());
    }

    @Override
    public int compareTo(Score o) {
        return new Integer(this.score).compareTo( o.score );
    }

}
