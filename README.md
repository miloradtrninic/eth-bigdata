# eth-bigdata

MapReduce 

- first Start docker containers from `/mapreduce` folder `docker-compose up`.
- second Run `./copy-files.sh`.
- third From hadoop namenode run `./compile-and-run.sh`.

Spark batch
- first Start docker containers from `/spark-batch` folder using `docker-compose up`.
- second Build application using maven build tool. 
- third Run `./copy-files.sh`
- forth Run run.sh with one of the params (org.bigdata.MaxByWeek, org.bigdata.MeanVariance, org.bigdata.MostConnectedAddresses)

Spark streaming
- first Start docker containers from `/spark-stream` folder using `docker-compose up`.
- second Build application using maven build tool. 
- Run `run.sh`
