package xlzsale.s

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession


import org.apache.spark._
import org.apache.spark.streaming._
object TestStreaming {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestDStream").setMaster("local[2]")

    val ssc = new StreamingContext(conf,Seconds(1))

      val lines=ssc.textFileStream("file:///D:/代码测试文本/log1.txt")
        val line=lines.flatMap(_.split(" "))
        val words=line.map(x=>(x,1)).reduceByKey(_ + _)
        words.print()
         ssc.start()
    ssc.awaitTermination()
  }
}
