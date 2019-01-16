package org.bigdata

import org.apache.spark.SparkContext
import org.joda.time.DateTime

object MaxByWeek {

  case class Transaction(blockId: Int,from: String, to: String, value: BigInt)
  case class Block(blockId: Int, timeStamp: Long)
  case class Key(year: Int, week: Int)
  def parseLineTransaction(line: String) = {
    val fields = line.split(",")
    //blockid, from, to, value
    val transaction: Transaction = Transaction(fields(3).toInt, fields(5), fields(6), BigInt(fields(7)))
    (fields(3).toInt, transaction)
  }

  def parseLineBlock(line: String) = {
    val fields = line.split(",")

    val block: Block = Block(fields(0).toInt, fields(16).toLong)
    //blockid, from, to, value

    (fields(0).toInt, block)
  }

  def getMax(t1: Transaction, t2: Transaction): Transaction = {
    if(t1.value > t2.value) {
      return t1
    }
    else return t2
  }

  def mapToTimestamp(blockTrans : (Int, (Transaction, Block))) = {
    val date : DateTime = new DateTime(blockTrans._2._2.timeStamp*1000L)
    (Key(date.getYear, date.getWeekOfWeekyear), blockTrans._2._1)
  }

  def main(args: Array[String]) {
    val sc = new SparkContext("local[*]", "MaxByWeek")

    val transactions = sc.textFile("hdfs://namenode:8020/maxbyweek/transactions.csv").filter(l => !l.contains("address")).map(parseLineTransaction)

    val rddBlocks = sc.textFile("hdfs://namenode:8020/maxbyweek/blocks.csv").filter(l => !l.contains("number")).map(parseLineBlock)

    val rdd = transactions.join(rddBlocks)

    rdd.persist()

    //rdd.saveAsTextFile("hdfs://namenode:8020/maxbyweek/joinresults")

    //rdd.map(mapToTimestamp).saveAsTextFile("hdfs://namenode:8020/maxbyweek/timestamp")

    rdd.map(mapToTimestamp).reduceByKey(getMax).saveAsTextFile("hdfs://namenode:8020/maxbyweek/results")


  }

}
