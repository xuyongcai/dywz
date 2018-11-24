package com.bigdata.dywz.classfy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * @author: xiaochai
 * @create: 2018-11-22
 **/
public class MovieClassifyMapper extends Mapper<LongWritable, Text, Text, DistanceAndLabel> {

    private DistanceAndLabel distance_label = new DistanceAndLabel();
    private String splitter = "";
    ArrayList<String> testData = new ArrayList<String>();
    private String testPath = "";

    /**
     * 读取测试数据
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        splitter = conf.get("SPLITTER");
        testPath = conf.get("TESTPATH");

        //读取测试数据存于列表testData中
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream is = fs.open(new Path(testPath));
        BufferedReader br = new BufferedReader(new InputStreamReader(is));

        String line = "";
        while ((line = br.readLine()) != null){
            testData.add(line);
        }

        is.close();
        br.close();
    }

    /**
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        double distance = 0;

        //读取训练数据
        String[] vals = value.toString().split(splitter);
        String[] singleTrainData = Arrays.copyOfRange(vals, 5, vals.length);
        String label = vals[1];

        for (String td : testData){
            String[] test = td.split(splitter);
            String[] singleTestData = Arrays.copyOfRange(test, 5, test.length);
            distance = Distance(singleTrainData, singleTestData);

            distance_label.setDistance(distance);
            distance_label.setLabel(label);

            //输出测试数据，以及测试数据与训练数据的距离和训练数据的类别
            //(5892,0,...,2)->(2.48,1)
            context.write(new Text(td), distance_label);
        }
    }

    /**
     * 计算训练数据和测试数据的距离
     * @param singleTrainData
     * @param singleTestData
     * @return
     */
    private double Distance(String[] singleTrainData, String[] singleTestData) {
        double sum = 0;
        for (int i=0; i<singleTrainData.length; i++){
            sum += Math.pow(Double.parseDouble(singleTrainData[i]),
                    Double.parseDouble(singleTestData[i]));
        }
        return Math.sqrt(sum);
    }
}
