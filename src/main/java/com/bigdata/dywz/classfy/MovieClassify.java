package com.bigdata.dywz.classfy;

import com.bigdata.dywz.util.JarUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * 6.驱动类Driver,实现用户性别分类
 * @author: xiaochai
 * @create: 2018-11-22
 **/
public class MovieClassify extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 5){
            System.err.println("demo.MovieClassify <testInput> <trainInput> <output> <k> <splitter>");
            System.exit(-1);
        }

        //创建configuration
        Configuration conf = getMyConfiguration();
        conf.setInt("K", Integer.parseInt(args[3]));
        conf.set("SPLITTER", args[4]);
        conf.set("TESTPATH", args[0]);

        //创建Job
        Job job = Job.getInstance(conf, "movie_knn");

        //设置Job的处理类
        job.setJarByClass(MovieClassify.class);

        //设置Mapper相关
        job.setMapperClass(MovieClassifyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DistanceAndLabel.class);

        //设置Reducer类和相关参数
        job.setReducerClass(MovieClassifyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //设置输入路径
        FileInputFormat.addInputPath(job, new Path(args[1]));

        //删除已存在的输出目录
        Path outputpath = new Path(args[2]);
        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(outputpath)){
            fileSystem.delete(outputpath, true);
        }
        //设置输出路径
        FileOutputFormat.setOutputPath(job, outputpath);

        //提交任务
        return job.waitForCompletion(true) ? -1 : 1;
    }

    /**
     * 设置连接hadoop集群的配置
     * @return
     */
    public static Configuration getMyConfiguration() {
        Configuration conf = new Configuration();
        conf.setBoolean("mapreduce.app-submission.cross-platform", true);
        conf.set("fs.defaultFS", "hdfs://localhost:9000"); //指定namenode
        conf.set("mapreduce.framework.name","yarn"); //指定使用yarn框架

        String resourcenode = "localhost";
        conf.set("yarn.resourcemanager.address", resourcenode + ":8032"); //指定resourcemannager
        conf.set("yarn.resourcemanager.scheduer.address", resourcenode + ":8030"); //指定资源分配器
        conf.set("mapreduce.jobhistory.address", resourcenode + ":10020");
        conf.set("mapreduce.job.jar", JarUtil.jar(MovieClassify.class));

        return conf;
    }

    public static void main(String[] args) {
        String[] myArgs={
          "/movie/testData",
          "/movie/trainData",
          "/movie/knnout",
          "3",
          ","
        };

        try {
            ToolRunner.run(getMyConfiguration(), new MovieClassify(), myArgs);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
