hdfs getconf -namenodes
O/P: servername
hadoop classpath
hdfs classpath
yarn classpath
O/P: class path details 

hdfs getconf -nnRpcAddresses

hadoop dfsadmin -report
-- this will generate the report like how many live datanodes and their details

hdfs dfs -put test1.txt
-- put a file to hdfs

hdfs dfs -ls
-- list all the file in hdfs

yarn application -list
-- list all running applications

spark-submit  --master local[*] --driver-memory 4g --num-executors 2 --executor-memory 2g test.py
-- run an application

yarn logs -applicationId <app ID>
-- view application log

aws emr add-steps --cluster-id ***** --steps Name=Spark,Jar=s3://bucketname/emr/*.jar,Args=[spark-submit,--deploy-mode,cluster,/home/hadoop/test.py] --profile lab
-- run spark application using aws



spark-submit   --master yarn --deploy-mode cluster --conf spark.executor.memory=4G --conf spark.driver.memory=2G  --conf spark.home=/usr/lib/spark  
--py-files /home/hadoop/pymodules/boto.zip test.py
--> submit job using module
first install boto3 using command in the master: pip install boto3 -t .
then zip all the folders zip boto.zip *
then use the zip as --py-files

run spark to connect to redshift
spark-submit  --jars /usr/share/aws/redshift/jdbc/redshift-jdbc4-1.2.37.1061.jar redshift.py


code:
import os
import sys
import pyspark
from pyspark.sql import SQLContext
from pyspark.context import SparkContext


sc = SparkContext.getOrCreate()
sql_context = SQLContext(sc)

df_sql = sql_context.read \
               .format("jdbc") \
               .option("url", "jdbc:redshift://url:5439/db") \
               .option("user", "username") \
               .option("password", "pwd") \
               .option("query", "select top 10 * from db.table") \
                           .option("driver","com.amazon.redshift.jdbc42.Driver") \
               .load()


print('shamim......')
print(df_sql)


