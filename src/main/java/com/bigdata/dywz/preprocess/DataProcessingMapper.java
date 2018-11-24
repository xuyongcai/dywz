package com.bigdata.dywz.preprocess;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * 4.数据清洗，缺失值、异常值都用0替换
 *
 * @author: xiaochai
 * @create: 2018-11-20
 **/
public  class DataProcessingMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private String splitter = "";

    enum DataProcessingCounter {
        NullData,
        AbnormalData
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        splitter = context.getConfiguration().get("SPLITTER");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //接收每一行数据，并按指定分割符进行分割
        String[] vals = value.toString().split(splitter);

        for (int i = 0; i< vals.length; i++){

            //判断每个字段的值是否为空，若为空则用0代替
            if(vals[i].equals("") || vals[i].equals("null") || vals[i].equals("NULL") || vals[i].equals("NAN")){

                context.getCounter(DataProcessingCounter.NullData).increment(1);
                vals[i] = "0";

            }else {
                context.getCounter (DataProcessingCounter.NullData).increment(0);
            }

            //判断每个字段的值是否为异常值，若是则用0代替
            if (Integer.parseInt(vals[i]) < 0) {
                context.getCounter(DataProcessingCounter.AbnormalData).increment(1);
                vals[i] = "0";

            }else {
                context.getCounter(DataProcessingCounter.AbnormalData).increment(0);
            }

        }


        StringBuffer result = new StringBuffer();

        //重新将字符串数组val 拼接成字符串
        for (int i = 0; i< vals.length; i++){
            if (i==0){
                result.append(vals[i]);
            }else {
                result.append(vals[i]).append(splitter);
            }
        }
        context.write(new Text(result.toString()), NullWritable.get());
    }
}