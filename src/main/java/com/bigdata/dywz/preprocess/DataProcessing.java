package com.bigdata.dywz.preprocess;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 4.数据清洗，缺失值、异常值都用0替换
 *
 * @author: xiaochai
 * @create: 2018-11-20
 **/
public class DataProcessing{

    public static class DataProcessingMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

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

            for (int i = 0; i < vals.length; i++) {

                //判断每个字段的值是否为空，若为空则用0代替
                if (vals[i].equals("") || vals[i].equals("null") || vals[i].equals("NULL") || vals[i].equals("NAN")) {

                    context.getCounter(DataProcessingCounter.NullData).increment(1);
                    vals[i] = "0";

                } else {
                    context.getCounter(DataProcessingCounter.NullData).increment(0);
                }

                //判断每个字段的值是否为异常值，若是则用0代替,如："-2"，"33-44"等都为异常
                if (vals[i].matches("[0-9]+")) {
                    context.getCounter(DataProcessingCounter.AbnormalData).increment(0);

                } else {
                    context.getCounter(DataProcessingCounter.AbnormalData).increment(1);
                    vals[i] = "0";
                }

            }


            StringBuffer result = new StringBuffer();

            //重新将字符串数组val 拼接成字符串
            for (int i = 0; i < vals.length; i++) {
                if (i == 0) {
                    result.append(vals[i]);
                } else {
                    result.append(splitter).append(vals[i]);
                }
            }
            context.write(new Text(result.toString()), NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if (args.length != 2){
            args = new String[2];
            args[0] = "/movie/movies_genres/part-r-00000";
            args[1] = "/movie/processing_out";
        }

        //创建configuration
        Configuration conf = new Configuration();
        conf.set("SPLITTER", ",");

        //创建Job
        Job job = Job.getInstance(conf, "data_processing");

        //设置Job的处理类
        job.setJarByClass(DataProcessing.class);

        //设置Mapper相关
        job.setMapperClass(DataProcessing.DataProcessingMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //设置Reducer类和相关参数
        job.setNumReduceTasks(0);

        //设置输入路径
        FileInputFormat.addInputPath(job, new Path(args[0]));

        //删除已存在的输出目录
        Path outputpath = new Path(args[1]);
        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(outputpath)){
            fileSystem.delete(outputpath, true);
        }
        //设置输出路径
        FileOutputFormat.setOutputPath(job, outputpath);

        //提交任务
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}