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

object RXCustomer {

    def main(args: Array[String]) {
      val  spark= SparkSession.builder().master("local[2]").appName("read mysql to hive").getOrCreate()
      //230,本地，以及208的mysql的链接URL
      val url230="jdbc:mysql://10.1.24.230:3306/biz_rx_new?"
      val url208="jdbc:mysql://10.1.24.208:3306/bicon_pcd?"
      val urllocal="jdbc:mysql://127.0.0.1:3306/automation?"
      //三个库的登录用户密码
      //230的
      val prop =new Properties()
      prop.setProperty("user","root")
      prop.setProperty("password","bicon@123")
      //208的
      val prop1 =new Properties()
      prop1.setProperty("user","root")
      prop1.setProperty("password","bicon@123")
      //本地的
      val prop3 =new Properties()
      prop3.setProperty("user","root")
      prop3.setProperty("password","root")

    //读取230润祥的三张需要用到的表
      val customer=spark.read.jdbc(url230,"bs_customer",prop)
      val city=spark.read.jdbc(url230,"base_city",prop)
      val province=spark.read.jdbc(url230,"base_province",prop)
    //读取208最新的客户和关系表
      val pcd_custom=spark.read.jdbc(url208,"pcd_custom_20190613_前全",prop1)
      val pcd_custom_relationship=spark.read.jdbc(url208,"pcd_custom_relationship_105862_20190613",prop1)
    //208读取表的临时表
      //pcd_custom.createOrReplaceTempView("pcd_custom")
      pcd_custom_relationship.createOrReplaceTempView("pcd_custom_relationship")
      //230读取表的临时表
     customer.createOrReplaceTempView("bs_customer")
     city.createOrReplaceTempView("base_city")
     province.createOrReplaceTempView("base_province")
      spark.udf.register("concat",(str:String)=>{
        val strr=str
        "1000".concat(strr)
      })
      //读取230表客户的8个字段
      val aa=spark.sql("SELECT concat(a.CUSTOMERID) as id,a.CUSTOMERNAME,a.CUSTOMERTYPE,a.REGISTERADDR,c.PROVINCENAME,b.cityname,a.LEGALPERSON,a.CONTACT FROM  `bs_customer` a  left join base_city b on a.cityid=b.CITYID left join base_province c on a.PROVINCEID=c.PROVINCEID")
          aa.createOrReplaceTempView("rx_customer_source")
      aa.show()
      //挑出新增客户
    val notinpcd_customer=spark.sql("select * from rx_customer_source a where not exists (select b.customid from pcd_custom_relationship b where a.id=b.customid)")
         notinpcd_customer.createOrReplaceTempView("rx_customer")
      notinpcd_customer.show()
      notinpcd_customer.write.mode("overwrite").jdbc(urllocal,"rx_customer_20190618_new",prop3)




     // aa.write.mode("overwrite").jdbc(urllocal,"rx_customer_source",prop3)

      //aa.createOrReplaceTempView("aa")
     // val aaa=spark.sql("select * from aa where id=1")
      //aaa.show()
    }

}
