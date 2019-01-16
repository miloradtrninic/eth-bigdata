package org.bigdata

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}

object MeanVariance {
  case class Transaction(hash: String,value: BigInt)

  def mapToTransaction(line: String) = {
    val fields = line.split(",")
    val value : Double = BigInt(fields(7)).doubleValue()
    Vectors.dense(value)
  }

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local[*]", "MeanVariance")
    val transactions = sc.textFile("hdfs://namenode:8020/maxbyweek/transactions.csv")

    val rdd = transactions.filter(l => !l.contains("address")).map(mapToTransaction)

    val summary: MultivariateStatisticalSummary = Statistics.colStats(rdd)

    printf("#############RESULTS#############\n\n\n")
    printf("Mean value: " + summary.mean + "\n")
    printf("Max value: " + summary.max + "\n")
    printf("Min value: " + summary.min + "\n")
    printf("Variance value: " + summary.variance + "\n")
    printf("Non zero values: " + summary.numNonzeros + "\n")
    printf("\n\n\n")
  }
}
