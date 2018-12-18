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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 3.对每个用户看过对电影类型进行统计
 * (1::F::1::10::48067::914::Musical|Romance) -> (1,1,1,10,48067,0,2,6,18,0,14,...)
 * @author: xiaochai
 * @create: 2018-11-23
 **/
public class MoviesGenres {

    public static class MoviesGenresMapper extends Mapper<LongWritable, Text, UserAndGender, Text>{
        private String splitter = "";
        private Text genres = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            splitter = context.getConfiguration().get("SPLITTER");
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            UserAndGender user_gender = new UserAndGender();

            String[] vals =  value.toString().split(splitter);
            user_gender.setUserID(vals[0]);

            if(vals[1].equals("M")){
                //性别为M则用0标记
                user_gender.setGender(0);
            }else {
                //性别为F则用1标记
                user_gender.setGender(1);
            }

            user_gender.setAge(Integer.parseInt(vals[2]));
            user_gender.setOccupation(vals[3]);
            user_gender.setZipCode(vals[4]);

            genres.set(vals[6]);

            context.write(user_gender, genres);
        }
    }

    public static class MoviesGenresReduser extends Reducer<UserAndGender, Text, Text, NullWritable>{

        @Override
        protected void reduce(UserAndGender key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            //初始化一个hashMap集合，集合中的键为18种电影类型，每个键对应的值为0
            HashMap<String, Integer> genresCounts = new HashMap<String, Integer>();
            String[] genreslist = {"Action","Adventure","Animation","Children's","Comedy","Crime",
                    "Documentary","Drama","Fantasy","Film-Noir","Horror","Musical","Mystery","Romance",
                    "Sci-Fi","Thriller","War","Western"};

            for (int i = 0; i < genreslist.length; i++){
                if (!genresCounts.containsKey(genreslist[i])){
                    genresCounts.put(genreslist[i], 0);
                }
            }

            //遍历值列表
            for (Text val : values){
                //对每个元素进行分割
                String[] genres = val.toString().split("\\|");
                for (int i = 0; i < genres.length; i++){
                    //如果hashmap元素的键包含分割结果的元素，则该键对应的值加1
                    if (genresCounts.containsKey(genres[i])){
                        genresCounts.put(genres[i], genresCounts.get(genres[i]) + 1);
                    }
                }
            }

            //将hashmap集合中所有键对应的值根据逗号连接成字符串
            StringBuffer result = new StringBuffer();
            for (Map.Entry<String, Integer> kv : genresCounts.entrySet()){
                if (result.length() == 0){
                    result.append(kv.getValue().toString());
                }else {
                    result.append(",").append(kv.getValue().toString());
                }
            }

            context.write(new Text(key.toString() + "," + result), NullWritable.get());
        }
    }

    /**
     * 定义driver
     * @param args
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if (args.length != 2){
            args = new String[2];
            args[0] = "/movie/users_movies/part-r-00000";
            args[1] = "/movie/movies_genres";
        }

        //创建configuration
        Configuration conf = new Configuration();
        conf.set("SPLITTER", "::");

        //创建Job
        Job job = Job.getInstance(conf, "movies_genres");

        //设置Job的处理类
        job.setJarByClass(MoviesGenres.class);

        //设置Mapper相关
        job.setMapperClass(MoviesGenresMapper.class);
        job.setMapOutputKeyClass(UserAndGender.class);
        job.setMapOutputValueClass(Text.class);

        //设置Reducer类和相关参数
        job.setReducerClass(MoviesGenresReduser.class);
        job.setOutputKeyClass(Text.class);
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
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
