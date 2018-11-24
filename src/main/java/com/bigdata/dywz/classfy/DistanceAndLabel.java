package com.bigdata.dywz.classfy;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义值类型（距离和类别的组合）
 * @author: xiaochai
 * @create: 2018-11-22
 **/
public class DistanceAndLabel implements Writable {

    private double distance;
    private String label;

    public DistanceAndLabel(){

    }
    public DistanceAndLabel(double distance, String label){
        this.distance = distance;
        this.label = label;
    }

    public double getDistance() {
        return distance;
    }

    public void setDistance(double distance) {
        this.distance = distance;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }


    /**
     * 先把distance写入out输出流，再把label写入out输出流
     * @param dataOutput
     * @throws IOException
     */
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(distance);
        dataOutput.writeUTF(label);
    }

    /**
     * 先读取距离，在读取类别
     * @param dataInput
     * @throws IOException
     */
    public void readFields(DataInput dataInput) throws IOException {
        this.distance = dataInput.readDouble();
        this.label = dataInput.readUTF();
    }

    /**
     * 使用逗号将距离和类别连接处字符串, 如：（2.48,1）
     * @return
     */
    @Override
    public String toString() {
        return this.distance + "," + this.label;
    }
}
