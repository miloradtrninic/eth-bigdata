# eth-bigdata

MapReduce 

- Start docker containers from `/mapreduce` folder `docker-compose up`.
- Run `./copy-files.sh`.
- From hadoop namenode run `./compile-and-run.sh`.

Spark batch
- Start docker containers from `/spark-batch` folder using `docker-compose up`.
- Build application using maven build tool. 
- Run `./copy-files.sh`
- Run `run.sh` with one of the params (org.bigdata.MaxByWeek, org.bigdata.MeanVariance, org.bigdata.MostConnectedAddresses)

Spark streaming
- Start docker containers from `/spark-stream` folder using `docker-compose up`.
- Build application using maven build tool. 
- Run `run.sh`
