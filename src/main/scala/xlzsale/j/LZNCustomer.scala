package xlzsale.j

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2019/6/17 0017.
  */
object LZNCustomer {
    def main(args: Array[String]) {
      val  spark= SparkSession.builder().master("local[*]").enableHiveSupport().config("spark.sql.crossJoin.enabled","true").appName("read mysql to hive").getOrCreate()
      val url230="jdbc:mysql://10.1.24.230:3306/biz_lzn?"
      //三个库的登录用户密码
      //230的
      val prop =new Properties()
      prop.setProperty("user","root")
      prop.setProperty("password","bicon@123")
    //读取230百川的三张需要用到的表
//      val customer=spark.read.jdbc(url230,"`基础_客商信息`",prop)
       val sale_fact=spark.read.jdbc(url230,"( select * from `库存_库房商品流水` where  `日期`>'2018-01-01') t",prop)
//      val goods=spark.read.jdbc(url230,"`基础_商品信息`",prop)
//val stock=spark.read.jdbc(url230,"( select * from `结账_库存日报` where  `日期`>'2018-01-01') t",prop)
    //230读取表的临时表
//      customer.createOrReplaceTempView("customer")
     sale_fact.createOrReplaceTempView("sale_fact")
//      goods.createOrReplaceTempView("goods")
    //  stock.createOrReplaceTempView("stock")
      //截取日期字符串
      spark.udf.register("sub",(str:String)=>{
        val ss=str.trim.substring(0,10)
        ss
      })
      spark.udf.register("concat1",(str1:Int,str2:Int,str3:String,str4:String,str5:String)=>{
       val ss=str1.toString+str2.toString+str3.toString+str4.toString+str5.toString
        ss
      })
      //中文括号转英文
      spark.udf.register("parentheses",(str:String)=>{
        val ss=str.trim().replace("（","(").replace("）",")").toString
        ss
      })
      //Double转Int
      spark.udf.register("toINT",(double: Double)=>{
        val ss=double.toInt
        ss
      })
      spark.udf.register("toDou",(double: Double)=>{
        val ss=double.toDouble
        ss
      })
//  val customer_a0=spark.sql("select id customerid,parentheses(`名称`)customername,parentheses(`企业类型`) customertype from customer")
  val sale_fact_a0=spark.sql("select concat1(`商品id`,`库房ID`,`批号`,`批次`,`日期`) rowkey,id saledtlid,`单据id` saleid,`业务类型` saletype,`商户id` customerid,`商品id` goodsid,`库房ID` warehouseid,`单位` goodsunit,toDou(`单价`) price,toINT(`数量`) goodsqty,toDou(`金额`) money,`批号` lots,`批次` batch,sub(`有效期至`) latestdate,`业务员ID` salerid,sub(`日期`) cfr_date,toINT(`结余数量`) surplusqty from sale_fact")
//  val goods_a0=spark.sql("select id goodsid,parentheses(`名称`) goodsname,parentheses(`规格`) goodsspetype,parentheses(`单位`) goodsunit,parentheses(`生产厂商`) vendorname,parentheses(`批准文号`) approvedno,`商品类型` goodstype from goods")
  //  val stock_a0=spark.sql("select concat1(`商品id`,`库房ID`,`批号`,`批次`,`日期`) rowkey,`商品id` goodsid,`库房ID` warehouseid,toINT(`数量`) surplusqty,toDou(`金额`) money,`批号` lots,`批次` batch,sub(`有效期至`) latestdate,sub(`日期`) cfr_date from stock")
    //  stock_a0.show()
      // 导入表到hive
 //     spark.sql("use aftest ")
//     customer_a0.repartition(1).write.mode("overwrite").option("timestampFormat","yyyy-MM-dd").orc("hdfs://nameservice1/user/hive/warehouse/aftest.db/w_lzn_customer")
   sale_fact_a0.repartition(1).write.mode("overwrite").option("timestampFormat","yyyy-MM-dd").orc("hdfs://nameservice1/user/hive/warehouse/aftest.db/w_lzn_sale_fact_one")
//      goods_a0.repartition(1).write.mode("overwrite").option("timestampFormat","yyyy-MM-dd").orc("hdfs://nameservice1/user/hive/warehouse/aftest.db/w_lzn_goods")
  //   stock_a0.repartition(1).write.mode("overwrite").option("timestampFormat","yyyy-MM-dd").orc("hdfs://nameservice1/user/hive/warehouse/aftest.db/w_lzn_stock_one")
  }

}
