package org.bigdata

import org.apache.spark.SparkContext
import org.apache.spark.graphx._

object MostConnectedAddresses {

  case class Transaction(blockId: Int,from: String, to: String, value: BigInt)
  case class Block(blockId: Int, timeStamp: Long)
  def makeVertex(line: String) : Array[(VertexId, String)] = {
    val fields = line.split(',')
    val hashFrom = java.util.UUID.nameUUIDFromBytes(fields(5).getBytes()).getMostSignificantBits
    val hashTo = java.util.UUID.nameUUIDFromBytes(fields(6).getBytes()).getMostSignificantBits
    return Array((hashFrom, fields(5)),(hashTo, fields(6)))
  }

  def makeEdges(line: String) : Edge[BigInt] = {
    val fields = line.split(',')
    val vertexIdFrom = java.util.UUID.nameUUIDFromBytes(fields(5).getBytes()).getMostSignificantBits
    val vertexIdTo = java.util.UUID.nameUUIDFromBytes(fields(6).getBytes()).getMostSignificantBits
    return Edge(vertexIdFrom, vertexIdTo, BigInt(fields(7)))
  }

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]", "MostConnected")
    val transactions = sc.textFile("hdfs://namenode:8020/maxbyweek/transactions.csv")
    val transactionsFiltered = transactions.filter(f => !f.contains("address"))

    val vertices = transactionsFiltered.flatMap(makeVertex).distinct()

    val edges = transactionsFiltered.map(makeEdges)

    val graph = Graph(vertices, edges, "")

    graph.degrees.join(vertices).sortBy(_._2._1, ascending = false).saveAsTextFile("hdfs://namenode:8020/mostconnected/results")

    if(args.length > 0) {
      val initialGraph = graph.mapVertices((id, address) => if (address == args(0)) 0.0 else Double.PositiveInfinity)
      val bfs = initialGraph.pregel(Double.PositiveInfinity, 10)(
        (id, attr, msg) => math.min(attr, msg),
        triplet => {
          if (triplet.srcAttr != Double.PositiveInfinity) {
            Iterator((triplet.dstId, triplet.srcAttr + 1))
          } else {
            Iterator.empty
          }
        },
        (a, b) => math.min(a, b))
      // Print out the first 100 results:
      bfs.vertices.join(vertices).saveAsTextFile("hdfs://namenode:8020/mostconnected/degreesofseparation")
    }

  }
}
