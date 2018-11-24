package com.bigdata.dywz.validate;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author: xiaochai
 * @create: 2018-11-24
 **/
public class ValidateMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    private String splitter = "";

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        splitter = context.getConfiguration().get("SPLITTER");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] vals = value.toString().split(splitter);
        context.write(NullWritable.get(), new Text(vals[0] + splitter + vals[2]));
    }
}
