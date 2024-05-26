# Apache Spark分布式集群安装

## 1、 基础环境安装
Spark既可以自己运行，也可以在几个现有的集群管理器上运行。它目前提供了几个部署选项：<br>
- local模式： 常用于本地开发测试，本地还分为local单线程和local-cluster多线程;
- Standalone Deploy Mode:<br>独立部署模式，在私有集群上部署Spark的最简单方法，典型的Mater/slave模式，Master可能有单点故障的；Spark支持ZooKeeper来实现 HA。
- Apache Mesos：（已弃用）<br>一个通用的集群管理器，也可以运行HadoopMapReduce和服务应用程序。
- Hadoop YARN：<br>Hadoop3中的资源管理器。运行在 yarn 资源管理器框架之上，由 yarn 负责资源管理，Spark 负责任务调度和计算。
- Kubernetes：
### 1.1 安全
默认情况下，不会启用身份验证等安全功能，在部署一个集群时，对internet或不受信任的网络是开开放的。
确保对群集的访问安全以防止未经授权的应用程序在群集上运行非常重要。

### 1.2 安装JAVA
Spark在Java 8/11/17、Scala 2.12/21.3、Python 3.8+和R 3.5+上运行。从Spark 3.5.0开始，不赞成使用8u371版本之前的版本。
~~~ shell
# 检查系统自带java，如果版并不符合则卸载
yum -y list java*
yum list installed | grep java
yum -y remove java-1.*
yum -y remove tzdata-java.noarch
yum -y remove javapackages-tools.noarch

# 安装1.8版本的JDK
## https://www.oracle.com/cn/java/technologies/downloads/
## https://download.oracle.com/otn/java/jdk/8u381-b09/8c876547113c4e4aab3c868e9e0ec572/jdk-8u381-linux-x64.tar.gz?AuthParam=1695712558_bb656d182f8a493a775f95a7e077121e
tar -zvxf /soft/jdk-8u381-linux-x64.tar.gz -C /usr/local/
mv /usr/local/jdk1.8.0_381 /usr/local/java

#配置环境变量
[root@ef69bca31ddb ~]cat /etc/profile
# 编辑vi /etc/profile，在最后增加
export JAVA_HOME=/usr/local/java
export CLASSPATH=.:$JAVA_HOME/lib/dt.jar:$JAVA_HOME/lib/tools.jar:$JAVA_HOME/jre/lib/rt.jar
export PATH=$PATH:$JAVA_HOME/bin
---
[root@ef69bca31ddb ~]source /etc/profile
[root@ef69bca31ddb ~]# java -version
java version "1.8.0_381"
Java(TM) SE Runtime Environment (build 1.8.0_381-b09)
Java HotSpot(TM) 64-Bit Server VM (build 25.381-b09, mixed mode)
[root@ef69bca31ddb ~]#
[root@ef69bca31ddb ~]# whereis java
java: /usr/local/java /usr/local/java/bin/java
[root@ef69bca31ddb ~]
~~~

### 1.2 安装Spark
- **下载Hadoop包**
~~~ html
Download Apache Spark™: https://spark.apache.org/downloads.html
https://www.apache.org/dyn/closer.lua/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3-scala2.13.tgz
~~~
- **安装Spark包**
~~~
[root@data-master01 ~]# tar -zvxf /soft/spark-3.5.0-bin-hadoop3-scala2.13.tgz -C /usr/local/
[root@data-master01 ~]# mv /usr/local/spark-3.5.0-bin-hadoop3-scala2.13 /usr/local/spark
[root@data-master01 ~]# vi /etc/profile
# 添加如下内容
export SPARK_HOME=/usr/local/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
[root@data-master01 ~]# source /etc/profile
[root@data-master01 ~]# spark-shell
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.5.0
      /_/

Using Scala version 2.13.8 (Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_381)
Type in expressions to have them evaluated.
Type :help for more information.
23/09/29 11:47:38 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Spark context Web UI available at http://data-master01:4040
Spark context available as 'sc' (master = local[*], app id = local-1695959259302).
Spark session available as 'spark'.

scala> println("你好,Spark！")
你好,Spark！
scala> 
~~~

## 2、 Spark集群搭建
### 2.1 local模式提交
使用spark-submit命令运行，local模式适合做本地测试，运行一些jar包等等。参数说明：
- --class 表示要执行程序入口主类；
- --master local[*] 部署模式，默认为本地模式，中括号表示分配的虚拟 CPU 核数量；
- 指定提交的jar包；
- 数字3   是用于设置当前应用的任务数量；
~~~ shell
[root@data-master01 ~]# spark-submit \
--class  org.apache.spark.examples.SparkPi \
--master local[*] \
/usr/local/spark/examples/jars/spark-examples_2.13-3.5.0.jar \
3
...
23/09/29 12:06:15 INFO TaskSchedulerImpl: Killing all running tasks in stage 0: Stage finished
23/09/29 12:06:15 INFO DAGScheduler: Job 0 finished: reduce at SparkPi.scala:38, took 0.816857 s
**Pi is roughly 3.1433757168785843**
23/09/29 12:06:15 INFO SparkContext: SparkContext is stopping with exitCode 0.
23/09/29 12:06:15 INFO SparkUI: Stopped Spark web UI at http://data-master01:4040
...
[root@data-master01 ~]# 
~~~
### 2.2 Standalone模式

Spark自身节点运行的模式，也称为独立部署模式。
- 修改config文件及环境参数
~~~
[root@data-master01 ~]# cd /usr/local/spark/
[root@data-master01 spark]# cp ./conf/spark-defaults.conf.template ./conf/spark-defaults.conf
[root@data-master01 spark]# cp ./conf/spark-env.sh.template ./conf/spark-env.sh
[root@data-master01 spark]# cp ./conf/workers.template ./conf/workers
[root@data-master01 spark]#
## 修改spark-defaults.conf，添加如下内容
[root@data-master01 spark]# vi ./conf/spark-defaults.conf
spark.master            spark://data-master01:9077
spark.executor.memory   2g
spark.eventLog.enabled  true
spark.serializer        org.apache.spark.serializer.KryoSerializer

## 修改spark-env.sh，添加如下内容
[root@data-master01 spark]# vi ./conf/spark-env.sh
export JAVA_HOME=/usr/local/java
export HADOOP_HOME=/usr/local/hadoop
export HADOOP_CONF_DIR=/usr/local/etc/hadoop

SPARK_MASTER_HOST=data-master01
SPARK_MASTER_PORT=9077

## 修改./conf/workers，添加如下内容
[root@data-master01 spark]# vi ./conf/workers
data-master01
data-worker01
data-worker02
~~~

- 分发config文件到data-worker01和data-worker02环境
~~~
[root@data-master01 local]# cd /usr/local
[root@data-master01 local]# scp -r spark/ data-worker01:/usr/local/
[root@data-master01 local]# scp -r spark/ data-worker02:/usr/local/
~~~
- 启动standalone模式
~~~
[root@data-master01 ~]# start-spark-all.sh
starting org.apache.spark.deploy.master.Master, logging to /usr/local/spark/logs/spark-root-org.apache.spark.deploy.master.Master-1-data-master01.out
data-master01: starting org.apache.spark.deploy.worker.Worker, logging to /usr/local/spark/logs/spark-root-org.apache.spark.deploy.worker.Worker-1-data-master01.out
data-worker02: starting org.apache.spark.deploy.worker.Worker, logging to /usr/local/spark/logs/spark-root-org.apache.spark.deploy.worker.Worker-1-data-worker02.out
data-worker01: starting org.apache.spark.deploy.worker.Worker, logging to /usr/local/spark/logs/spark-root-org.apache.spark.deploy.worker.Worker-1-data-worker01.out
~~~
打开页面查看状态：
http://data-master01:8080/

- 验证standalone模式

因为Hadoop和Spark在同一个机器中部署，两者的启动脚步都是start-all.sh，所以本环境中
将./spark/sbin/start-all.sh脚步名称，修改为start-spark-all.sh。
使用 standalone 模式提交任务，圆周率官方案例：
~~~
[root@data-master01 ~]# spark-submit --class org.apache.spark.examples.SparkPi \
--master spark://data-master01:9077 \
/usr/local/spark/examples/jars/spark-examples_2.13-3.5.0.jar \
3 

23/09/29 17:21:18 INFO SparkContext: Running Spark version 3.5.0
23/09/29 17:21:18 INFO SparkContext: OS info Linux, 5.14.0-316.el9.x86_64, amd64
23/09/29 17:21:18 INFO SparkContext: Java version 1.8.0_381
...
23/09/29 17:30:17 INFO TaskSchedulerImpl: Killing all running tasks in stage 0: Stage finished
23/09/29 17:30:17 INFO DAGScheduler: Job 0 finished: reduce at SparkPi.scala:38, took 5.560039 s
Pi is roughly 3.141410471368238
23/09/29 17:30:17 INFO SparkContext: SparkContext is stopping with exitCode 0.
23/09/29 17:30:17 INFO BlockManagerMasterEndpoint: Registering block manager 192.168.0.40:45847 with 413.9 MiB RAM, BlockManagerId(2, 192.168.0.40, 45847, None)
23/09/29 17:30:17 INFO SparkUI: Stopped Spark web UI at http://data-master01:4040
...
~~~

### 2.3 YARN 模式
YARN 模式建立在 standalone 模式之上，所以这里就不重复赘述了。请先部署好 standalone 模式，然后在其基础上来建立 YARN 模式。


参考：
https://blog.csdn.net/u011109589/article/details/124855282
https://blog.csdn.net/weixin_46389691/article/details/127514291
