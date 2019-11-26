#TaskA:
#Find the average number of bytes for lines of each response code.

#TaskB:
#Count the number of log entries that happen between two given timestamps.

#to perform tasks,spark and python2 should be working on the system.

#enter the following command to execute the file:
spark-submit SparkSQL.py file://<file-location> <timestamp1> <timestamp2>

#for eg:
spark-submit SparkSQL.py file:///home/hdp/Desktop/CS226/Assignment3/nasa.tsv 804571212 804571400
