package xlzsale.j

import org.apache.spark.sql.SparkSession

object TestSale1 {
  def main(args: Array[String]): Unit = {


  val  spark= SparkSession.builder().master("local[2]").enableHiveSupport().appName("read hive to ddd").getOrCreate()



  spark.sql("use aftest")
//第一种，先读取外部表
spark.sql("create table dut3(id int, content string) partitioned by (dt string) row format delimited fields terminated by '\\t' lines terminated by '\\n'")


  //LOAD DATA local INPATH '/user/hua/*' INTO TABLE day_hour partition(dt='2010-07- 07');
  /* val df=spark.sql("create table new customer" +
   "" +
   "")*/
  // df.write.mode("overwrite").jdbc(urllocal,"xy_customer20190814",prop3)
    /* df.repartition(1).write.mode("overwrite").option("timestampFormat","yyyy-MM-dd")
    .orc("hdfs://nameservice1/user/hive/warehouse/aftest.db/w_bs_sale_fact_orc")*/
  }
}
