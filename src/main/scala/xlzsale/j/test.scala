package xlzsale.j

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DoubleType

/**
  * Created by Administrator on 2019/7/5 0005.
  */
object test {
  def main(args: Array[String]) {

    val sparkSession=SparkSession.builder().master("local").appName("").getOrCreate()
    import sparkSession.implicits._

    val df= sparkSession.sparkContext.parallelize(Seq(
      (10,10)
    )).toDF("id","age")
    df.printSchema()


    import org.apache.spark.sql.functions._
    val res0=df.select(col("id").cast(DoubleType),(col("age")/3).cast(DoubleType))
    res0.show()
    res0.printSchema()
//    println(getYesterdayMonthHive())
  }
  def getYesterdayMonthHive():String= {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMM")
    Calendar.getInstance()
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    var yesterday = dateFormat.format(cal.getTime())
    yesterday
  }
}
