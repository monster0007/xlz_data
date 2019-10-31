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

object SJHTCustomer {

    def main(args: Array[String]) {
      val  spark= SparkSession.builder().master("local[2]").appName("read mysql to hive").getOrCreate()
      //230,本地，以及208的mysql的链接URL
      val url230="jdbc:oracle:thin:@10.1.24.218:1521:sjhterp?"
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

    //读取230百川的三张需要用到的表
      val customer=spark.read.jdbc(url230,"`基础_客商信息`",prop)
    //230读取表的临时表
      customer.createOrReplaceTempView("`基础_客商信息`")

      //读取208最新的客户和关系表
      //val pcd_custom=spark.read.jdbc(url208,"pcd_custom_20190613_前全",prop1)
      val pcd_custom_relationship=spark.read.jdbc(url208,"pcd_custom_relationship_105862_20190613",prop1)
    //208读取表的临时表
     // pcd_custom.createOrReplaceTempView("pcd_custom") 可拿可不拿
      pcd_custom_relationship.createOrReplaceTempView("pcd_custom_relationship")


      spark.udf.register("concat",(str:String)=>{
        "n5000".concat(str)
      })
      //读取230表客户的8个字段 insert into pcd_custom_20190514(id,customname,customtype,address,city,contactname,phone)
      //select id,名称,企业类型,地址,固定电话,联系人,移动电话 from `基础_客商信息`;
      val a0=spark.sql("select concat(id) as id,`名称` as customname,`企业类型` as customtype,`地址` as address,`固定电话` as city,`联系人` as contactname,`移动电话` as phone from `基础_客商信息`")
          a0.createOrReplaceTempView("lzn_customer_source1")
     //a0.select(a0("id"),a0("customname"),a0("customtype"),a0("address"),a0("contactname"),a0("ifnull(phone,city)"))
      a0.show()
      val a1=spark.sql("select id,customname,customtype,address,contactname ,ifnull(phone,city) as phone  from lzn_customer_source1")
          a1.createOrReplaceTempView("lzn_customer_source")
      a1.show()
      //挑出新增客户
    val notinpcd_customer=spark.sql("select * from lzn_customer_source a where not exists (select b.customid from pcd_custom_relationship b where a.id=b.customid)")
         //notinpcd_customer.createOrReplaceTempView("bc_customer")
      notinpcd_customer.show()
      notinpcd_customer.write.mode("overwrite").jdbc(urllocal,"lzn_customer_source",prop3)




     // aa.write.mode("overwrite").jdbc(urllocal,"rx_customer_source",prop3)

      //aa.createOrReplaceTempView("aa")
     // val aaa=spark.sql("select * from aa where id=1")
      //aaa.show()
    }

}
