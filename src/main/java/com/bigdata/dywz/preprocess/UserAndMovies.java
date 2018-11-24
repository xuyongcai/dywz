package com.bigdata.dywz.preprocess;

/**
 * @author: xiaochai
 * @create: 2018-11-24
 **/
public class UserAndMovies {
    private String userID;
    private String gender;
    private Integer age;
    private String occupation;
    private String zipCode;
    private String movieID;
    private String genres;

    private String flag;// 表的标记

    public String getUserID() {
        return userID;
    }

    public void setUserID(String userID) {
        this.userID = userID;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public String getOccupation() {
        return occupation;
    }

    public void setOccupation(String occupation) {
        this.occupation = occupation;
    }

    public String getZipCode() {
        return zipCode;
    }

    public void setZipCode(String zipCode) {
        this.zipCode = zipCode;
    }

    public String getMovieID() {
        return movieID;
    }

    public void setMovieID(String movieID) {
        this.movieID = movieID;
    }

    public String getGenres() {
        return genres;
    }

    public void setGenres(String genres) {
        this.genres = genres;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    @Override
    public String toString() {
        return userID + "::" + gender + "::" + age + "::" + occupation + "::"
                + zipCode + "::" + movieID + "::" + genres;
    }
}

