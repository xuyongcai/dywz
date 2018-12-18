package com.bigdata.dywz.preprocess;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

/**
 * 1.连接users.dat数据和ratings.dat数据
 * @author: xiaochai
 * @create: 2018-11-24
 **/
public class JoinUsersAndRatings {

    public static class JoinMapper extends Mapper<LongWritable, Text, Text, UserAndMovies>{

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
            if (name.startsWith("users")){ //用户信息数据处理,users.dat

                user_movies.setUserID(values[0]);
                user_movies.setGender(values[1]);
                user_movies.setAge(Integer.parseInt(values[2]));
                user_movies.setOccupation(values[3]);
                user_movies.setZipCode(values[4]);
                user_movies.setMovieID("");
                user_movies.setGenres("");
                user_movies.setFlag("users");

                k.set(values[0]);

            }else if (name.startsWith("ratings")){ //用户对电影的部分评分数据处理,ratings.dat
                user_movies.setUserID(values[0]);
                user_movies.setGender("");
                user_movies.setAge(-1);
                user_movies.setOccupation("");
                user_movies.setZipCode("");
                user_movies.setMovieID(values[1]);
                user_movies.setGenres("");
                user_movies.setFlag("ratings");

                k.set(values[0]);

            }

            context.write(k, user_movies);
        }
    }

    public static class JoinReducer extends Reducer<Text, UserAndMovies, Text, NullWritable>{

        @Override
        protected void reduce(Text key, Iterable<UserAndMovies> values, Context context) throws IOException, InterruptedException {

            //放user的数据
            UserAndMovies user_movies = new UserAndMovies();
            //放ratings的数据
            ArrayList<UserAndMovies> list = new ArrayList<UserAndMovies>();

            //分离数据
            for (UserAndMovies um : values){

                if("users".equals(um.getFlag())){

                    user_movies.setUserID(um.getUserID());
                    user_movies.setGender(um.getGender());
                    user_movies.setAge(um.getAge());
                    user_movies.setOccupation(um.getOccupation());
                    user_movies.setZipCode(um.getZipCode());

                }else {
                    UserAndMovies rating = new UserAndMovies();

                    rating.setUserID(um.getUserID());
                    rating.setMovieID(um.getMovieID());

                    list.add(rating);
                }
            }

            //拼接数据
            for (UserAndMovies um : list) {

                um.setGender(user_movies.getGender());
                um.setAge(user_movies.getAge());
                um.setOccupation(user_movies.getOccupation());
                um.setZipCode(user_movies.getZipCode());

                context.write(new Text(um.toString()), NullWritable.get());
            }
        }
    }

    /**
     * 定义driver
     * @param args
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if (args.length != 3){
            args = new String[3];
            args[0] = "/movie/origin_data/users.dat";
            args[1] = "/movie/origin_data/ratings.dat";
            args[2] = "/movie/users_ratings";
        }

        //创建configuration
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name","yarn"); //指定使用yarn框架


        //创建Job
        Job job = Job.getInstance(conf, "join_users_and_ratings");

        //设置Job的处理类
        job.setJarByClass(JoinUsersAndRatings.class);

        //设置Mapper相关
        job.setMapperClass(JoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(UserAndMovies.class);

        //设置Reducer类和相关参数
        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //设置输入路径
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileInputFormat.addInputPath(job, new Path(args[0]));

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
