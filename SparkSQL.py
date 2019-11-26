import sys
import time
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import *



conf = SparkConf().setMaster("local").setAppName("test")
sc = SparkContext(conf=conf)
spark = SparkSession \
    .builder \
    .appName("SparkSQL") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

path,time1,time2 = (sys.argv[1],sys.argv[2],sys.argv[3])

#path= "file:///home/hdp/Desktop/CS226/Assignment3/nasa.tsv" 


lines = sc.textFile(path)


parts=lines.map(lambda x: x.split('\t'))

parts.take(1)


log=parts.map(lambda x: Row(host=str(x[0]),logname=str(x[1]),time=int(x[2]),method=str(x[3]),URL= str(x[4]),response= str(x[5]), bytes=int(x[6])))

log_df=spark.createDataFrame(log)



log_df.registerTempTable("log")

start = int(round(time.time() * 1000))

value=spark.sql("select avg(bytes) as average,response  from log group by response").collect()

#val1=spark.sql("select avg(bytes) as val from log where response==200").collect()[0].asDict()['val']

#val2=spark.sql("select avg(bytes) as val from log where response==304").collect()[0].val #row.columnName to get val

#val3=spark.sql("select avg(bytes) as val from log where response==404").collect()[0].val

#val4=spark.sql("select avg(bytes) as val from log where response==302").collect()[0].val

end =int(round(time.time() * 1000))

response1,value1=value[0].response,value[0].average
response2,value2=value[1].response,value[1].average
response3,value3=value[2].response,value[2].average
response4,value4=value[3].response,value[3].average


taskATime=end-start


f = open("taskA.txt", "w")
f.write("Code %s, average number of bytes = %f\n"%(response1,value1))
f.write("Code %s, average number of bytes = %f\n"%(response4,value4))
f.write("Code %s, average number of bytes = %f\n"%(response3,value3))
f.write("Code %s, average number of bytes = %f\n"%(response2,value2))
f.write("\nTask A using SparkSQL = %f ms\n"%(taskATime))
f.close()




start2=int(round(time.time() * 1000))


log_count=spark.sql("SELECT count(*) as val from log where time>={0} and time<={1}".format(time1,time2)).collect()[0].val

end2 = int(round(time.time() * 1000))

taskBTime=end2-start2

f = open("taskB.txt", "w")

f.write("Number of log entries between %s and %s timestamps = %d\n"%(time1, time2, log_count))

f.write("\nTask B using SparkSQL = %f ms\n"%(taskBTime))

f.close()
