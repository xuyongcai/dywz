#!/bin/bash
#工作流运行shell脚本


#上传需要的数据到hdfs
#hadoop fs -mkdir -p /movie/origin_data
#hadoop fs -put /Users/xuyongcai/IdeaProjects/dywz/data/users.dat /movie/origin_data
#hadoop fs -put /Users/xuyongcai/IdeaProjects/dywz/data/ratings.dat /movie/origin_data
#hadoop fs -put /Users/xuyongcai/IdeaProjects/dywz/data/movies.dat /movie/origin_data

#-------------数据预处理-------------------

#1.连接users.dat数据和ratings.dat数据
users_ratings_i_01=/movie/origin_data/users.dat
users_ratings_i_02=/movie/origin_data/ratings.dat
users_ratings_o=/movie/users_ratings

hadoop jar /Users/xuyongcai/IdeaProjects/dywz/target/dywz-1.0.jar com.bigdata.dywz.preprocess.JoinUsersAndRatings \
$users_ratings_i_01 $users_ratings_i_02 $users_ratings_o


#2.连接movies.dat数据与users_ratings数据
users_movies_i_01=/movie/users_ratings/part-r-00000
users_movies_i_02=/movie/origin_data/movies.dat
users_movies_o=/movie/users_movies

hadoop jar /Users/xuyongcai/IdeaProjects/dywz/target/dywz-1.0.jar com.bigdata.dywz.preprocess.JoinRatingsUsersAndMovies \
$users_movies_i_01 $users_movies_i_02 $users_movies_o


# 3.对每个用户看过对电影类型进行统计
# (1::F::1::10::48067::914::Musical|Romance) -> (1,1,1,10,48067,0,2,6,18,0,14,...)
movies_genres_i=/movie/users_movies/part-r-00000
movies_genres_o=/movie/movies_genres

hadoop jar /Users/xuyongcai/IdeaProjects/dywz/target/dywz-1.0.jar com.bigdata.dywz.preprocess.MoviesGenres \
$movies_genres_i $movies_genres_o

#4.数据清洗，缺失值、异常值都用0替换
processing_i=/movie/movies_genres/part-r-00000
processing_o=/movie/processing_out

hadoop jar /Users/xuyongcai/IdeaProjects/dywz/target/dywz-1.0.jar com.bigdata.dywz.preprocess.MoviesGenres \
$processing_i $processing_o

#5.划分数据集类
hadoop jar /Users/xuyongcai/IdeaProjects/dywz/target/dywz-1.0.jar com.bigdata.dywz.preprocess.DivieDataSet

#-----------------------实现用户性别分类---------------------
#6.实现用户性别分类
hadoop jar /Users/xuyongcai/IdeaProjects/dywz/target/dywz-1.0.jar com.bigdata.dywz.classfy.MovieClassify


#-----------------------评价分类结果的准确性---------------------
#7.评价分类结果的准确性
#hadoop jar /Users/xuyongcai/IdeaProjects/dywz/target/dywz-1.0.jar com.bigdata.dywz.validate.Validate


#-----------------------寻找最优K值---------------------
#hadoop jar /Users/xuyongcai/IdeaProjects/dywz/target/dywz-1.0.jar com.bigdata.dywz.validate.AllJob

