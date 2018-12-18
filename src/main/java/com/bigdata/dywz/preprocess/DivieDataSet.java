package com.bigdata.dywz.preprocess;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * 5.划分数据集类
 *
 * @author: xiaochai
 * @create: 2018-11-21
 **/
public class DivieDataSet {

    /**
     * 读取原始数据并统计数据的记录数
     *
     * @param fs
     * @param path
     * @return
     * @throws IOException
     */
    public static int getSize(FileSystem fs, Path path) throws IOException {
        int count = 0;

        FSDataInputStream is = fs.open(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(is));

        String line = "";

        while((line = br.readLine()) != null){
            count++;
        }
        br.close();
        is.close();
        return count;
    }

    /**
     * 训练数据集
     * 随机获取80%原始数据的对应下标
     * （第1条，第2条，第3条）-> <0,1,2>
     * @param count
     * @return
     */
    public static Set<Integer> trainIndex(int count){
        Set<Integer> train_index = new HashSet<Integer>();
        int trainSplitNum = (int) (count * 0.8);

        Random random = new Random();
        while (train_index.size() < trainSplitNum){
            train_index.add(random.nextInt(count));
        }
        return train_index;
    }


    /**
     * 验证数据集
     * 随机获取10%原始数据对应的下标
     * @param count
     * @param train_index
     * @return
     */
    public static Set<Integer> validateIndex(int count, Set<Integer> train_index){
        Set<Integer> validate_index = new HashSet<Integer>();
        int validataeSplitNum = count - (int) (count * 0.9);

        Random random = new Random();
        while (validate_index.size() < validataeSplitNum){
            int a = random.nextInt(count);
            if (!train_index.contains(a)){
                validate_index.add(a);
            }
        }
        return validate_index;
    }

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "localhost:9000");
        FileSystem fs = FileSystem.get(conf);

        //获取预处理后的电影数据路径
        Path movieData = new Path("/movie/processing_out/part-m-00000");

        //得到电影数据大小
        int dataSize = getSize(fs, movieData);

        //得到train数据对应原始数据的下标
        Set<Integer> train_index = trainIndex(dataSize);

        //得到validate数据对应原始数据的下标
        Set<Integer> validate_index = validateIndex(dataSize, train_index);

        //训练数据存放的路径
        Path train = new Path("hdfs://localhost:9000/movie/trainData");
        fs.delete(train, true);
        FSDataOutputStream os1 = fs.create(train);
        BufferedWriter bw1 = new BufferedWriter(new OutputStreamWriter(os1));


        //测试数据存放的路径
        Path test = new Path("hdfs://localhost:9000/movie/testData");
        fs.delete(test, true);
        FSDataOutputStream os2 = fs.create(test);
        BufferedWriter bw2 = new BufferedWriter(new OutputStreamWriter(os2));

        //验证数据存放的路径
        Path validate = new Path("hdfs://localhost:9000/movie/validateData");
        fs.delete(validate, true);
        FSDataOutputStream os3 = fs.create(validate);
        BufferedWriter bw3 = new BufferedWriter(new OutputStreamWriter(os3));

        //读取数据并将数据分为训练数据集、测试数据集、以及验证数据集写入到HDFS
        FSDataInputStream is = fs.open(movieData);
        BufferedReader br = new BufferedReader(new InputStreamReader(is));

        String line = "";
        int sum = 0;
        while ((line = br.readLine()) != null){

            if(train_index.contains(sum)){
                bw1.write(line.toString());
                bw1.newLine();

            }else if(validate_index.contains(sum)){
                bw3.write(line.toString());
                bw3.newLine();

            }else {
                bw2.write(line.toString());
                bw2.newLine();
            }
            sum += 1;
        }
        bw1.close();
        os1.close();
        bw2.close();
        os2.close();
        bw3.close();
        os3.close();
        br.close();
        is.close();
        fs.close();
    }
}
