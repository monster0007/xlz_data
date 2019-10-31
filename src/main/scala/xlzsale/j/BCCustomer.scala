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

object BCCustomer {

    def main(args: Array[String]) {
      val  spark= SparkSession.builder().master("local[*]").appName("read mysql to hive").getOrCreate()
      //230,本地，以及208的mysql的链接URL
      val url230="jdbc:mysql://10.1.24.230:3306/biz_bc?"
      val url208="jdbc:mysql://10.1.24.208:3306/bicon_pcd?"
      val urllocal="jdbc:mysql://10.1.24.208:3306/automation?"
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
      val customer=spark.read.jdbc(url230,"infowldw",prop)

    //读取208最新的客户和关系表
      //val pcd_custom=spark.read.jdbc(url208,"pcd_custom_20190613_前全",prop1)
      val pcd_custom_relationship=spark.read.jdbc(url208,"pcd_custom_relationship_105862_20190613",prop1)
    //208读取表的临时表
     // pcd_custom.createOrReplaceTempView("pcd_custom") 可拿可不拿
      pcd_custom_relationship.createOrReplaceTempView("pcd_custom_relationship")
      //230读取表的临时表
     customer.createOrReplaceTempView("infowldw")

      spark.udf.register("concat",(str:String)=>{

        "3000".concat(str)
      })
      //读取230表客户的8个字段 id,customname,customtype,address,city,contactname,phone
      val aa=spark.sql("SELECT concat(WangLDWID) as id,WangLDWMC as customname,fenl as customtype,ShouHDZ as address,city,ShouHR as contactname,ShouHDH as phone from infowldw")
          aa.createOrReplaceTempView("bc_customer_source")
     // aa.show()
      //挑出新增客户
    val notinpcd_customer=spark.sql("select * from bc_customer_source a where not exists (select b.customid from pcd_custom_relationship b where a.id=b.customid)")
         //notinpcd_customer.createOrReplaceTempView("bc_customer")
     // notinpcd_customer.show()
     // notinpcd_customer.write.mode("overwrite").jdbc(urllocal,"bc_customer_20190618",prop3)

      val area=spark.read.jdbc(urllocal,"area",prop)
      area.createOrReplaceTempView("area")
      spark.udf.register("updatec5",(str:String)=>{
        try{

          if(str==null || str==' ' || str=='0'){
            "22222222222222222222222222222222222222222222222"
          }else {
            "33333333333333333333333333333333333333333333"
            val sss1 = spark.sql("select pid from area where id=" + str)
            "444444444444444444444444444444444444"
            /*if (sss1.select("id")==null||sss1.select("pid")==null){
              ""
            }else{*/
            if (sss1.select("pid") != null) {
              val a1 = sss1.select("pid").toString().toInt
              if (a1 < 34) {
                val c = str
                c.toString()
              } else {
                val sss2 = spark.sql("select id,pid from area where id=" + a1)
                val a2 = sss2.select("pid").toString().toInt
                if (a2 < 34) {
                  val c2 = sss2.select("id")
                  c2.toString()
                } else {
                  val sss3 = spark.sql("select id,pid from area where id=" + a2)
                  val a3 = sss3.select("pid").toString()
                  a3
                }
              }
            }else{
              ""
            }
          }
        }catch {
          case e: NullPointerException => str

        }
      })
spark.sql("select updatec5(id) from area ").limit(20).show()
     // aa.write.mode("overwrite").jdbc(urllocal,"rx_customer_source",prop3)

      //aa.createOrReplaceTempView("aa")
     // val aaa=spark.sql("select * from aa where id=1")
      //aaa.show()
    }

}
