package com.bigdata.dywz.preprocess;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

/**
 * 2.连接movies.dat数据与ratings_users数据
 * @author: xiaochai
 * @create: 2018-11-24
 **/
public class JoinRatingsUsersAndMovies {

    public static class JoinMapper extends Mapper<LongWritable, Text, Text, UserAndMovies> {

        Text k = new Text();
        String name = "";

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            //获取文件名
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            name = fileSplit.getPath().getName();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] values = value.toString().split("::");
            UserAndMovies user_movies = new UserAndMovies();

            //将文件名作为两个文件数据的标识
            if(name.startsWith("movies")){ //部分电影信息数据处理,movies.dat
                user_movies.setMovieID(values[0]);
                user_movies.setGenres(values[2]);
                user_movies.setFlag("movies");

                k.set(values[0]);

            }else { //用户电影评分和用户信息数据,ratings_users

                user_movies.setUserID(values[0]);
                user_movies.setGender(values[1]);
                user_movies.setAge(Integer.parseInt(values[2]));
                user_movies.setOccupation(values[3]);
                user_movies.setZipCode(values[4]);
                user_movies.setMovieID(values[5]);
                user_movies.setFlag("ratings_users");

                k.set(values[5]);

            }

            context.write(k, user_movies);
        }
    }

    public static class JoinReducer extends Reducer<Text, UserAndMovies, Text, NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<UserAndMovies> values, Context context) throws IOException, InterruptedException {

            //放movie的数据
            UserAndMovies user_movies = new UserAndMovies();
            //放ratings_users的数据
            ArrayList<UserAndMovies> list = new ArrayList<UserAndMovies>();

            //分离数据
            for (UserAndMovies um : values){

                if("movies".equals(um.getFlag())){
                    try {
                        BeanUtils.copyProperties(user_movies, um);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }else {
                    UserAndMovies ratings_user = new UserAndMovies();

                    try {
                        BeanUtils.copyProperties(ratings_user, um);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    list.add(ratings_user);
                }
            }

            //拼接数据
            for (UserAndMovies um : list) {

                um.setGenres(user_movies.getGenres());

                context.write(new Text(um.toString()), NullWritable.get());
            }
        }
    }

    /**
     * 定义driver
     * @param args
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //创建configuration
        Configuration conf = new Configuration();

        //创建Job
        Job job = Job.getInstance(conf, "join_ratingsusers_and_movies");

        //设置Job的处理类
        job.setJarByClass(JoinRatingsUsersAndMovies.class);

        //设置Mapper相关
        job.setMapperClass(JoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(UserAndMovies.class);

        //设置Reducer类和相关参数
        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //设置输入路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        //删除已存在的输出目录
        Path outputpath = new Path(args[2]);
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
