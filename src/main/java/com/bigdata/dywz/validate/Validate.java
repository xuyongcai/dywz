package com.bigdata.dywz.validate;

import com.bigdata.dywz.util.JarUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 7.z
 * @author: xiaochai
 * @create: 2018-11-24
 **/
public class Validate extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 3){
            System.out.println("com.bigdata.dywz.validate.Validate <input> <output> <splitter>");
            System.exit(-1);
        }

        Configuration conf = getMyConfiguration();
        conf.set("SPLITTER", args[2]);

        //创建Job
        Job job = Job.getInstance(conf, "validate");

        //设置Job的处理类
        job.setJarByClass(Validate.class);

        //设置Mapper相关
        job.setMapperClass(ValidateMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        //设置Reducer类和相关参数
        job.setReducerClass(ValidateReducer.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(NullWritable.class);

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
        return job.waitForCompletion(true) ? 0 : 1;

    }

    /**
     * 设置连接hadoop集群的配置
     * @return
     */
    public static Configuration getMyConfiguration(){
        Configuration conf = new Configuration();
        conf.setBoolean("mapreduce.app-submission.cross-platform", true);
        conf.set("fs.defaultFS", "hdfs://localhost:9000"); //指定namenode
        conf.set("mapreduce.framework.name","yarn"); //指定使用yarn框架

        String resourcenode = "localhost";
        conf.set("yarn.resourcemanager.address", resourcenode + ":8032"); //指定resourcemannager
        conf.set("yarn.resourcemanager.scheduer.address", resourcenode + ":8030"); //指定资源分配器
        conf.set("mapreduce.jobhistory.address", resourcenode + ":10020");
        conf.set("mapreduce.job.jar", JarUtil.jar(Validate.class));

        return conf;
    }

    public static void main(String[] args) {
        String[] myArgs = {
                "/movie/knnout/part-r-00000",
                "/movie/validateout",
                ","
        };

        try {
            ToolRunner.run(getMyConfiguration(), new Validate(), myArgs);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
