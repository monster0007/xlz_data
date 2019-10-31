package xlzsale.j

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2019/6/17 0017.
  */
/*val sqltable="select * from student "
val df=spark.read.format("jdbc").option("url",url)
  .option("driver","com.mysql.jdbc.Driver")
  .option("user","root")
  .option("password","root")
  .option("dbtable","("+sqltable+")t").load()*/

object NOCityDealCustomer {

  def main(args: Array[String]) {
    val  spark= SparkSession.builder().master("local[2]").appName("read mysql to hive").getOrCreate()

    val urllocal="jdbc:mysql://127.0.0.1:3306/automation?"

    val prop3 =new Properties()
    prop3.setProperty("user","root")
    prop3.setProperty("password","root")

    //读取230百川的三张需要用到的表
    val pcd_new_customer_no_city=spark.read.jdbc(urllocal,"pcd_new_customer_no_city",prop3)

    pcd_new_customer_no_city.createOrReplaceTempView("pcd_new_customer_no_city")

   /* spark.udf.register("updatep1",(str:String)=>{
      try{
        val p1=str.indexOf('省')
        if(p1>0){
        val tmp=str.substring(0,p1)

        //val tmp=str.substring('省',1)
       // val res=str.replaceAll(tmp,"")
        tmp}
      }catch {
        case e:NullPointerException=>null
      }
    })*/


    //updatep1(address) as province,
   val b11=spark.sql("select id,customname,customtype,address,updatec1(address) as city,contactname,phone from pcd_new_customer_no_city")
    b11.show()
   // b11.createOrReplaceTempView("kl_customer_source")

  }


}
