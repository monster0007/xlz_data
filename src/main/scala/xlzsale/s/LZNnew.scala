package xlzsale.s

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import org.apache.spark.sql.SparkSession

import scala.language.postfixOps

object LZNnew {

  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("lzn").master("local[*]").enableHiveSupport().getOrCreate()
    //添加隐式方法，（不用schema就可以将list转为DF）
    import spark.implicits._
    val url230="jdbc:mysql://10.1.24.230:3306/biz_lzn?"
    //三个库的登录用户密码
    //230的
    val prop =new Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","bicon@123")
    prop.setProperty("driver", "com.mysql.jdbc.Driver")

    val cal =Calendar.getInstance()
    cal.clear();//清楚系统时间，避免影响
    var list:List[String]=List()
    //循环年月
    for(i<- 2018 to 2019){
      for(j <- 1 to 12  ){
        //设置成3月的第0天，也就是2月的最后一天
        cal.set(i,j,0);
        val ss=  if(j<10){
          //拿到日期函数，如 2019-01-31
          cal.get(Calendar.YEAR)+"-0"+(cal.get(Calendar.MONTH)+1)+"-"+cal.get(Calendar.DAY_OF_MONTH).toString
        }else{
          cal.get(Calendar.YEAR)+"-"+(cal.get(Calendar.MONTH)+1)+"-"+cal.get(Calendar.DAY_OF_MONTH).toString
        }
        list=list.::(ss)
      }
    }
    //list转换为DF
    spark.sparkContext.parallelize(list).toDF("date").createOrReplaceTempView("tmp")
    //截取日期字符串,年月日
    spark.udf.register("sub",(str:String)=>{
      val ss=str.trim.substring(0,10)
      ss
    })
    //截取日期字符串,年月 201801
    spark.udf.register("sub3",(str:String)=>{
      val ss=str.trim.substring(0,7).replace("-","")
      ss.toString()
    })
    //截取日期字符串,年月 2018-01
    spark.udf.register("sub1",(str:String)=>{
      val ss=str.trim.substring(0,7)
      ss.toString()
    })
    //截取日期字符串,年月 2018-01 返回 2017-12
    spark.udf.register("sub2",(str1:String)=>{
        val dfs:SimpleDateFormat = new SimpleDateFormat("yyyy-MM")
        var cal: Calendar= Calendar.getInstance
        val ss=str1.split("-")
        cal.set(ss(0).toInt,ss(1).toInt,0)
        cal.add(Calendar.MONTH,-1)
        dfs.format(cal.getTime)
    })

    spark.udf.register("concat1",(str1:Int,str2:Int,str3:String,str4:String)=>{
      val ss=str1.toString+str2.toString+str3.toString+str4.toString
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

    //读取230百川的三张需要用到的表
    val day="2019-08"
    val dt=day.replaceAll("-","")
    val sale_fact=spark.read.jdbc(url230,s"( select * from `库存_库房商品流水` where  substr(`日期`,1,7)='$day') t",prop)
    val stock=spark.read.jdbc(url230,"( select * from `结账_库存日报` where  `日期`>'2019-01-01') t",prop)
    //230读取表的临时表
    sale_fact.createOrReplaceTempView("sale_fact")
    stock.createOrReplaceTempView("stock")


    //销售事实表
    val sale_fact_a0=spark.sql("select id saledtlid,`单据id` saleid,`业务类型` saletype,`商户id` customerid,`商品id` goodsid,`库房ID` warehouseid,`单位` goodsunit,toDou(`单价`) price,toINT(`数量`) goodsqty,toDou(`金额`) money,`批号` lots,`批次` batch,sub(`有效期至`) latestdate,`业务员ID` salerid,sub(`日期`) cfr_date,sub3(`日期`) cfr_month,sub1(`日期`) nowmonth,sub2(sub1(`日期`)) lastmonth,toINT(`结余数量`) surplusqty from sale_fact")
    sale_fact_a0.createOrReplaceTempView("sale_fact_a0")
     //库存表
      val stock_a0=spark.sql("select `商品id` goodsid,`库房ID` warehouseid,toINT(`数量`) surplusqty,toDou(`金额`) money,`批号` lots,`批次` batch,sub(`有效期至`) latestdate,sub(`日期`) cfr_date,sub1(`日期`) cfr_month from stock")
     stock_a0.createOrReplaceTempView("stock_a0")
    spark.sql("select date from tmp union select max(sub(`日期`)) from stock").createOrReplaceTempView("date_tmp")
    spark.sql("select * from date_tmp order by date").show(500)
    //每个月最后一天的库存
    // val stock_a1_1=spark.sql("select a.goodsid,a.warehouseid,a.surplusqty,a.money,a.lots,a.batch,a.latestdate,a.cfr_date,b.date,a.cfr_month from stock_a0 a left join tmp b on a.cfr_date=b.date where b.date is not null ")
   //stock_a1_1.show()
    val stock_a1_2=spark.sql("select a.goodsid,sum(a.surplusqty) surplusqty,a.cfr_date,b.date,a.cfr_month from stock_a0 a left join tmp b on a.cfr_date=b.date where b.date is not null group by a.goodsid,a.cfr_date,b.date,a.cfr_month")
   // stock_a1_2.show()
    stock_a1_2.createOrReplaceTempView("stock_a1")
    //处理过后的销售事实表
    val sale_fact_a2=spark.sql("select a.saledtlid,a.saleid,a.saletype,a.customerid,a.goodsid,a.warehouseid,a.goodsunit,a.price,a.goodsqty,a.money,a.lots,a.batch,a.latestdate,a.salerid,a.cfr_date,a.cfr_month,a.surplusqty,ifnull(b.surplusqty,0) nowqty,ifnull(c.surplusqty,0) lastqty from  sale_fact_a0  a left join stock_a1 b on a.goodsid=b.goodsid and a.nowmonth=b.cfr_month LEFT JOIN stock_a1 c on  a.goodsid=c.goodsid and a.lastmonth=c.cfr_month order by a.goodsid")
   // sale_fact_a2.show()
    // 导入表到hive
     spark.sql("use aftest ")
    sale_fact_a2.repartition(1).write.mode("overwrite").option("timestampFormat","yyyy-MM-dd").orc(s"hdfs://nameservice1/user/hive/warehouse/aftest.db/w_lzn_sale_fact_five/dt=$dt")
    // stock_a1.repartition(1).write.mode("overwrite").option("timestampFormat","yyyy-MM-dd").orc("hdfs://nameservice1/user/hive/warehouse/aftest.db/w_lzn_stock_three")
   // sale_fact_a0.repartition(1).write.mode("overwrite").option("timestampFormat","yyyy-MM-dd").orc("hdfs://nameservice1/user/hive/warehouse/aftest.db/w_lzn_sale_fact_four")
   spark.sql(s"alter table w_lzn_sale_fact_five add if not exists partition(dt=$dt)")









  }
}
