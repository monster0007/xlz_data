package xlzsale.j

import org.apache.spark.sql.SparkSession

object TestSale {
  def main(args: Array[String]) {
    val  spark= SparkSession.builder().master("local[2]").enableHiveSupport().appName("read hive to ddd").getOrCreate()
    //230,本地，以及208的mysql的链接URL
   /* val urllocal="jdbc:mysql://10.1.24.208:3306/aaa?"
    val prop3 =new Properties()
    prop3.setProperty("user","root")
    prop3.setProperty("password","bicon@123")*/


    spark.sql("use aftest")
    val df=spark.sql("select salesetdtlid,salesetid,goodsid,goodsname,unitprice,goodsqty,l_money,from_unixtime(cfr_date*3600*24,'yyyyMMdd') cfr_date,t_money,customerid,customername,create_uid,salerid from  w_bs_sale_fact")
    df.show()
   // df.write.mode("overwrite").jdbc(urllocal,"xy_customer20190814",prop3)
    df.repartition(1).write.mode("overwrite").option("timestampFormat","yyyy-MM-dd")
      .orc("hdfs://nameservice1/user/hive/warehouse/aftest.db/w_bs_sale_fact_orc")

  }
}

