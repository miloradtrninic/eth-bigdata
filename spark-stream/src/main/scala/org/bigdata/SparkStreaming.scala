package org.bigdata

import kafka.serializer.StringDecoder
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka.KafkaUtils
import play.api.libs.json._



object SparkStreaming {

  case class Transaction(blockHash: String,
                         blockNumber: Int,
                         from: String,
                         gas: Long,
                         gasPrice: BigInt,
                         hash: String,
                         input: String,
                         nonce: Long,
                         to: String,
                         transactionIndex: Int,
                         value: BigInt)
  def mapper(jsonString: (String, String)): Transaction = {
    implicit val myClassFormat = Json.format[Transaction]

    Json.parse(jsonString._2).as[Transaction]
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Eth")
    val ssc = new StreamingContext(conf, Seconds(1))


    val kafkaParams: Map[String, String] = Map("metadata.broker.list" -> "kafka:9092")

    val topics: Set[String] = List("ethereum").toSet

    val lines: DStream[String] = KafkaUtils.
      createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
      .map(_._2)

    val txs: DStream[(String, Int)] = lines.map(tx => (tx, 1))
    val txCount = txs.reduceByKeyAndWindow(_ + _, Seconds(300))

    txCount.foreachRDD {
      rdd =>
        rdd.collect().foreach(println)
    }

    ssc.start()
    ssc.awaitTermination()

  }


}
