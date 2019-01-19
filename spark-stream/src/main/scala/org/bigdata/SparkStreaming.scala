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
                         from: String,
                         to: String,
                         hash: String,
                         value: BigInt)

  def mapper(jsonString: String): Option[Transaction] = {
    val json = jsonString.substring(1, jsonString.length - 1)
    val result = Json.parse(json.replaceAll("\\\\", "")).validate[Transaction](Json.format[Transaction])
    result.asOpt
  }

  def toAddressKey(transaction: Option[Transaction]): List[(String, BigInt)] = {
    List((transaction.get.from, transaction.get.value), (transaction.get.to, transaction.get.value))
  }

  def update(values: Seq[BigInt], state: Option[BigInt]): Option[BigInt] = {
    val currentSum : BigInt = values.foldLeft(BigInt(0))(_+_)
    val previousSum: BigInt = state.getOrElse(0)

    Some(currentSum + previousSum)
  }


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Eth")
    val ssc = new StreamingContext(conf, Seconds(1))

    ssc.checkpoint("checkpoint")
    val kafkaParams: Map[String, String] = Map("metadata.broker.list" -> "kafka:9092")

    val topics: Set[String] = List("ethereum").toSet

    val lines = KafkaUtils.
      createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics).map(x => mapper(x._2))

    val addresses = lines.flatMap(toAddressKey)
      .reduceByKey(_ + _)
      .updateStateByKey(update)
      .foreachRDD {
        rdd =>
        rdd.collect().foreach(println)
    }
    ssc.start()
    ssc.awaitTermination()

  }


}
