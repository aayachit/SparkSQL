#task A and taskB:

#to perform these tasks,spark and python2 should be working on the system.

#enter the following command to execute the file:
spark-submit SparkSQL.py file://<file-location> <timestamp1> <timestamp2>

#for eg:
spark-submit SparkSQL.py file:///home/hdp/Desktop/CS226/Assignment3/nasa.tsv 804571212 804571400



#task C:

#to perform this task, AsterixDB should be running on the system.
#The permissions for file AsterixDB.sh should be changed to 755 as follows:

chmod 755 AsterixDB.sh

#enter the following command to execute the file:
./AsterixDB.sh 127.0.0.1:///<file-location> <timestamp1> <timestamp2>

#for eg:
./AsterixDB.sh 127.0.0.1:///home/hdp/Desktop/CS226/Assignment3/nasa.tsv 804571212 804571400

