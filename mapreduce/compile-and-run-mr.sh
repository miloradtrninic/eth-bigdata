DIR=/mapreduce

cd $DIR

printf "\nCOPY FILE TO HDFS\n"

$HADOOP_PREFIX/bin/hdfs dfs -mkdir /eth
$HADOOP_PREFIX/bin/hdfs dfs -put transactions.csv /eth/
$HADOOP_PREFIX/bin/hdfs dfs -put map.py /eth/
$HADOOP_PREFIX/bin/hdfs dfs -put reducer.py /eth/

printf "\nSETTING EXECUTEABLE PY\n"

chmod a+x *.py

cd $HADOOP_PREFIX

printf "\nRUN HADOOP-STREAMING\n"

bin/hadoop jar share/hadoop/tools/lib/hadoop-streaming-$HADOOP_VERSION.jar \
    -input /eth/transactions.csv \
    -output /eth_out \
    -mapper $DIR/map.py \
    -reducer $DIR/reducer.py
printf "\nRESULTS\n"

$HADOOP_PREFIX/bin/hdfs dfs -cat /eth_out/*
