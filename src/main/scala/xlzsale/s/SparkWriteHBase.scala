package xlzsale.s



import org.apache.spark._
import org.apache.spark.sql.{SQLContext, types}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.Row
object SparkWriteHBase {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkWriteHBase").setMaster("local")
    val sc = new SparkContext(sparkConf)
   val spark=new SQLContext(sc)
    import spark.implicits
   /* val people =sc.textFile("file:///D:/代码测试文本/student.txt")
    val ss=people.flatMap(x=>x.split(","))
      ss.persist()*/

    spark.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/test").option("driver","com.mysql.jdbc.Driver")
  .option("dbtable","student").option("root","root").option("password","root")
val  sturRDD=spark.sparkContext.parallelize(Array("1 zhangsan 男 23","2 lisi 男 30")).map(_.split(" "))
val  schema=StructType(List(StructField("id",IntegerType,true),StructField("name",StringType,true),StructField("sex",StringType,true),StructField("age",IntegerType,true)))
    val rowRDD=sturRDD.map(p=>Row(p(0).toInt,p(1).trim,p(2).trim(),p(3).toInt))
    val s=spark.createDataFrame(rowRDD,schema)

s.show()
   /* ss.foreach(println)
    ss.collect().foreach(println)
    println(people.count())
    println(people.collect().mkString("-"))*/













   /*val peoplelengths= people.map(x=>Row(x+":"+x.length))
    peoplelengths.foreach(x=>println(x))
    val linesize=people.flatMap(x=>x.split(","))
    linesize.foreach(x=>println(x))
    val biga=linesize.reduce((a,b)=>if (a>b) a else b)
    println(biga)*/
   // val pp= peoplelengths.reduce((a,b)=>a+b)


    //模式
    /*val schemastring="name,age"
    val schemaaa=StructType(schemastring.split(",").map(x=>StructField(x,StringType,true)))


    val rowRDD = people.map(_.split(",")).map(p => Row(p(0), p(1).trim))
    val peopleDataFrame = sqlcontext.createDataFrame(rowRDD,schemaaa)
    peopleDataFrame.createOrReplaceTempView("peopleDataFrame")
    val personsRDD = sqlcontext.sql("select name,age from peopleDataFrame where age>'20'").rdd
    personsRDD.foreach(t => println("Name:"+t(0)+",Age:"+t(1)))*/

  }
}
