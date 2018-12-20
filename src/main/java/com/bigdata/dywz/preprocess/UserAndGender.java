package com.bigdata.dywz.preprocess;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author: xiaochai
 * @create: 2018-11-23
 **/
public class UserAndGender implements WritableComparable<UserAndGender> {
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

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(userID);
        out.writeInt(gender);
        out.writeInt(age);
        out.writeUTF(occupation);
        out.writeUTF(zipCode);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.userID = in.readUTF();
        this.gender = in.readInt();
        this.age = in.readInt();
        this.occupation = in.readUTF();
        this.zipCode = in.readUTF();
    }

    @Override
    public int compareTo(UserAndGender o) {
        return this.userID.compareTo(o.getUserID());
    }
}
