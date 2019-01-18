$HADOOP_PREFIX/bin/hdfs dfs -mkdir /maxbyweek

$HADOOP_PREFIX/bin/hdfs dfs -mkdir /mostconnected

$HADOOP_PREFIX/bin/hdfs dfs -put maxbyweek/transactions.csv /maxbyweek/

$HADOOP_PREFIX/bin/hdfs dfs -put maxbyweek/blocks.csv /maxbyweek/
