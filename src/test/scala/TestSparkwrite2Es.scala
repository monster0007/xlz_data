import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.elasticsearch.spark._

class TestSparkwrite2Es {

  def sparkwrite2es(): Unit ={
    val spark = SparkSession.builder()
      .appName("w2es")
      .master("local[*]")
      .config("es.index.auto.create", "true")
      .config("es.nodes","10.1.24.211")
      .getOrCreate()
    val sc = spark.sparkContext
    val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
    val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")
    val sq = Seq(numbers,airports)
    println("sq:" + sq)
    val list = List()
     val re = numbers :: airports ::  list
    println("re:" + re)
    val ret = list.toSeq
    sc.makeRDD(
      ret
    ).saveToEs("spark/docs")
  }

}
object TestSparkwrite2Es{
  def main(args: Array[String]): Unit = {
    new TestSparkwrite2Es().sparkwrite2es()
  }
}