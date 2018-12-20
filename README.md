# dywz
电影网站用户性别预测----用knn算法和MapReduce实现。

# 环境要求
>1. fs.defaultFS -> hdfs://localhost:9000
>2. yarn.resourcemanager.address -> hdfs://localhost:8032
 

# 快速运行
>1. 进入data目录
>2. 打开run.sh进行编辑
>3. 修改users.dat，rating.dat，movies.dat文件的路径和需要运行的jar包的路径
>4. 执行run.sh