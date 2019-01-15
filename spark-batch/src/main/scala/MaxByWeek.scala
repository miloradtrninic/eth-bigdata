import org.apache.spark.SparkContext
import com.github.nscala_time.time.Imports._
object MaxByWeek {

  case class Transaction(blockId: BigInt,from: String, to: String, value: Int)
  case class Block(blockId: BigInt, timeStamp: BigInt)
  case class Key(year: Int, week: Int)
  def parseLineTransaction(line: String) = {
    val fields = line.split(",")
    //blockid, from, to, value
    val transaction: Transaction = Transaction(fields(3).toInt, fields(5), fields(6), fields(7).toInt)
    (fields(3).toInt, transaction)
  }

  def parseLineBlock(line: String) = {
    val fields = line.split(",")
    val block: Block = Block(fields(0).toInt, fields(16).toInt)
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

  val sc = new SparkContext("local[*]", "FriendsByAge")

  val transactions = sc.textFile("../transactions.csv").map(parseLineTransaction)

  val rddBlocks = sc.textFile("../blocks.csv").map(parseLineBlock)

  val rdd = transactions.join(rddBlocks)

  val results = rdd.map(mapToTimestamp).reduceByKey(getMax).collect()

  results.foreach(println)
}