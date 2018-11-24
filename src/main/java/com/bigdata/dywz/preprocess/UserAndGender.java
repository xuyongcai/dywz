package com.bigdata.dywz.preprocess;

/**
 * @author: xiaochai
 * @create: 2018-11-23
 **/
public class UserAndGender {
    private String userID;
    private Integer gender;
    private Integer age;
    private String occupation;
    private String zipCode;

    public String getUserID() {
        return userID;
    }

    public void setUserID(String userID) {
        this.userID = userID;
    }

    public Integer getGender() {
        return gender;
    }

    public void setGender(Integer gender) {
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

    @Override
    public String toString() {
        return userID + "," + gender + "," + age + "," + occupation + "," + zipCode;
    }
}
