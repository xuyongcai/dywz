package com.bigdata.dywz.validate;

import com.bigdata.dywz.classfy.MovieClassify;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * 寻找最优K值
 * @author: xiaochai
 * @create: 2018-11-27
 **/
public class AllJob {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000"); //指定namenode

        FileSystem fs = FileSystem.get(conf);
        double maxAccuracy = 0;
        int bestK = 0;
        int[] k = {2,3,5,9,15,30,55,70,80,100};

        for (int i=0; i<k.length; i++){
            double accuracy =0;
            String[] classifyArgs = {
                    "/movie/validateData",
                    "/movie/trainData",
                    "/movie/knnout",
                    String.valueOf(k[i]),
                    ","
            };

            try {
                ToolRunner.run(MovieClassify.getMyConfiguration(), new MovieClassify(), classifyArgs);
            } catch (Exception e) {
                e.printStackTrace();
            }

            String[] validateArgs = {
                    "/movie/knnout/part-r-00000",
                    "/movie/validateout",
                    ","
            };

            try {
                ToolRunner.run(Validate.getMyConfiguration(), new Validate(), validateArgs);
            } catch (Exception e) {
                e.printStackTrace();
            }

            FSDataInputStream is = fs.open(new Path("/movie/validateout/part-r-00000"));
            BufferedReader br = new BufferedReader(new InputStreamReader(is));

            String line = "";
            while ((line = br.readLine()) != null){
                accuracy = Double.valueOf(line);
            }

            br.close();
            is.close();

            if (accuracy > maxAccuracy){
                maxAccuracy = accuracy;
                bestK = k[i];
            }
            System.out.println("K=" + k[i] + "\t" + "accuracy=" + accuracy);
        }

        System.out.println("最优k值是：" + bestK + "\t" + "最优k值对应的准确率：" + maxAccuracy);
    }
}
