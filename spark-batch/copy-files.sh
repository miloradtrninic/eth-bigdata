docker cp ./transactions.csv spark-master:/transactions.csv
docker cp ./blocks.csv spark-master:/blocks.csv


docker exec spark-master /opt/hadoop-2.8.0/bin/hdfs dfs -mkdir /maxbyweek
docker exec spark-master /opt/hadoop-2.8.0/bin/hdfs dfs -mkdir /mostconnected
docker exec spark-master /opt/hadoop-2.8.0/bin/hdfs dfs -put transactions.csv /maxbyweek/
docker exec spark-master /opt/hadoop-2.8.0/bin/hdfs dfs -put blocks.csv /maxbyweek/



docker cp ./target/bigdata-1.0-SNAPSHOT-jar-with-dependencies.jar spark-master:/bigdata-1.0-SNAPSHOT-jar-with-dependencies.jar


#docker cp ./transactions.csv spark-master:/transactions.csv
#docker cp ./blocks.csv spark-master:/blocks.csv
