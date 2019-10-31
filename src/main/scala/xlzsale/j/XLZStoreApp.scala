package xlzsale.j

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import org.apache.spark.sql.SparkSession

object XLZStoreApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]").appName("").enableHiveSupport()
      .getOrCreate()

    val url230="jdbc:mysql://10.1.24.230:3306/biz_lzn?"

    // 230服务配置
    val prop =new Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","bicon@123")
    prop.setProperty("driver", "com.mysql.jdbc.Driver")

    // 209服务配置
    val proper =new Properties()
    proper.setProperty("user","root")
    proper.setProperty("password","123456")
    proper.setProperty("driver", "com.mysql.jdbc.Driver")

    val day="2019-09"

    //TODO...数据加工主代码

    XlzStoreFactApp(spark, url230, prop,day)


  }

  private def XlzStoreFactApp(spark: SparkSession, url230: String, prop: Properties,day:String) = {

    val dt=day.replaceAll("-","")
    //
    val storeDtlDF=spark.read.jdbc(url230,s"( select * from `库存_库房商品流水` ) t",prop)
    val customerDF = spark.read.jdbc(url230,"(select ID,`名称`,`企业类型` from 基础_客商信息 ) t",prop)
    val supplyDF = spark.read.jdbc(url230,"(select * from 基础_供应商信息) t",prop)
    val storeDF = spark.read.jdbc(url230,"(select * from 结账_库存日报  where  `日期`>'2018-12-01') t",prop)
    val saleTypeDF = spark.read.jdbc(url230,"(select * from 库存_库房商品流水 where `业务类型` = '采购入库') t",prop)
    val goods=spark.read.jdbc(url230,"基础_商品信息",prop)
    val changes=spark.read.jdbc(url230,"基础_商品单位换算率",prop)


   saleTypeDF.createOrReplaceTempView("sale_type")

    storeDtlDF.cache()

    storeDtlDF.createOrReplaceTempView("storeDtl")
    customerDF.createOrReplaceTempView("customer")
    supplyDF.createOrReplaceTempView("supply")
    storeDF.createOrReplaceTempView("store")
    goods.createOrReplaceTempView("goods")
    changes.createOrReplaceTempView("changes")

    val cal =Calendar.getInstance()
    cal.clear();//清楚系统时间，避免影响
    var list:List[String]=List()
    //循环年月
    for (i <- 2018 to 2019) {
      for (j <- 1 to 12) {
        //设置成3月的第0天，也就是2月的最后一天
        cal.set(i, j, 0);
        val ss = if (j < 10) {
          //拿到日期函数，如 2019-01-31
          cal.get(Calendar.YEAR) + "-0" + (cal.get(Calendar.MONTH) + 1) + "-" + cal.get(Calendar.DAY_OF_MONTH).toString
        } else {
          cal.get(Calendar.YEAR) + "-" + (cal.get(Calendar.MONTH) + 1) + "-" + cal.get(Calendar.DAY_OF_MONTH).toString
        }
        list = list.::(ss)
      }
    }

    //list转换为DF
    import spark.implicits._
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
    //


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
    //取负值
    spark.udf.register("falINT",(double: Double)=>{
      val ss=double.toInt
      -ss
    })

    // 加工销售渠道相关
    val custResultDF = spark.sql(
      """
        |select ID,`名称`,`企业类型`,
        |(case  when `企业类型` in ('零售','商业零售','农村药店','农村诊所') then '终端板块'
        |when `企业类型` in ('零售及连锁') then '连锁板块'
        |when `企业类型` in ('医疗机构','医院','卫生院','乡卫生院') then '医疗板块'
        |when `企业类型` in ('批发企业','商业批发','生产企业','其他') then '商业调拨'
        |else '商业调拨' end) as `销售渠道名称`
        |from customer
      """.stripMargin)
    // 加工销售渠道相关
    val suppResultDF = spark.sql(
      """
        |select ID,`名称`,`企业类型`,
        |(case  when `企业类型` in ('零售','商业零售','农村药店','农村诊所') then '终端板块'
        |when `企业类型` in ('零售及连锁') then '连锁板块'
        |when `企业类型` in ('医疗机构','医院','卫生院','乡卫生院') then '医疗板块'
        |when `企业类型` in ('批发企业','商业批发','生产企业','其他') then '商业调拨'
        |else '商业调拨' end) as `销售渠道名称`
        |from supply
      """.stripMargin)
    //处理与库存流水相关的加工逻辑

    custResultDF.createOrReplaceTempView("cust_tmp")
    suppResultDF.createOrReplaceTempView("supp_tmp")

    // 采购信息表
 spark.sql(
      """
        |select `商品id` goodsid,`库房ID` warehouseid,(`数量`) surplusqty,
        |(`金额`) money,`批号` lots,`批次` batch,
        |(`有效期至`) latestdate,(`日期`) cfr_date,
        |(`日期`) cfr_month,b.`名称`
        |from sale_type a
        |left join supply b
        |on a.`商户id` = b.ID
      """.stripMargin).createOrReplaceTempView("supply_tmp")
    // spark.sql("select * from supply_tmp").show(500)

    //销售事实表
    val sale_fact_a0=spark.sql(
      """
        |select id saledtlid,`单据id` saleid,`业务类型` saletype,`商户id` customerid,
        |`商品id` goodsid,`库房ID` warehouseid,`单位` goodsunit,toDou(`单价`) price,
        |(case when `业务类型` in ('销退验入','采退出库') then falINT(`数量`)
        |else toINT(`数量`) end) as goodsqty,
        |(case when `业务类型` in ('销退验入','采退出库') then falINT(`金额`)
        |else toINT(`金额`) end) as money,
        |`批号` lots,
        |`批次` batch,sub(`有效期至`) latestdate,`业务员ID` salerid,
        |sub(`日期`) cfr_date,sub3(`日期`) cfr_month,sub1(`日期`) nowmonth,
        |sub2(sub1(`日期`)) lastmonth,toINT(`结余数量`) surplusqty
        |
        |from storeDtl
      """.stripMargin)
    sale_fact_a0.createOrReplaceTempView("sale_fact_a0")

    //库存表
    val stock_a0=spark.sql(
      """
        |select  `商品id` goodsid,`库房ID` warehouseid,toINT(`数量`) surplusqty,
        |toDou(`金额`) money,`批号` lots,`批次` batch,
        |sub(`有效期至`) latestdate,sub(`日期`) cfr_date,
        |sub1(`日期`) cfr_month
        |from store
      """.stripMargin)

    // spark.sql("select * from tmp").show(500)
    spark.sql("select date from tmp union select max(sub(`日期`)) from store").createOrReplaceTempView("date_tmp")

    // spark.sql("select * from date_tmp order by date").show(500)

    stock_a0.createOrReplaceTempView("stock_a0")
    //每个月最后一天的库存
    val stock_a1=spark.sql(
      """
        |select a.goodsid,sum(a.surplusqty) surplusqty,
        |a.cfr_date,a.cfr_month
        |from stock_a0 a
        |left join date_tmp b
        |on a.cfr_date=b.date
        |where b.date is not null
        |group by a.goodsid,a.cfr_date,a.cfr_month
      """.stripMargin)
    stock_a1.createOrReplaceTempView("stock_a1")

    //处理过后的销售事实表
    //销售的销售事实表
    val sale_fact_a2=spark.sql(
      """
        |select a.saledtlid,a.saleid,
        |a.saletype,a.customerid,a.goodsid,a.warehouseid,
        |a.price,a.goodsqty,a.money,a.lots,a.batch,
        |a.latestdate,a.salerid,a.cfr_date,a.cfr_month,a.surplusqty,
        |ifnull(b.surplusqty,0) nowqty,ifnull(c.surplusqty,0) lastqty,
        |d.`销售渠道名称` as channel_name,ifnull(e.`名称`,'其他') as suppliername
        |from  sale_fact_a0  a
        |
        |left join stock_a1 b
        |on a.goodsid=b.goodsid
        |and a.nowmonth=b.cfr_month
        |
        |LEFT JOIN stock_a1 c
        |on a.goodsid=c.goodsid
        |and a.lastmonth=c.cfr_month
        |
        |left join cust_tmp d
        |on a.customerid = d.ID
        |
        |left join supply_tmp e
        |on a.goodsid =  e.goodsid
        |and a.batch = e.batch
        |and a.lots = e.lots
        |where a.saletype in ('销售出库','销退验入')
      """.stripMargin)
    //采购的销售事实表
    val sale_fact_a4=spark.sql(
      """
        |select a.saledtlid,a.saleid,
        |a.saletype,a.customerid,a.goodsid,a.warehouseid,
        |a.price,a.goodsqty,a.money,a.lots,a.batch,
        |a.latestdate,a.salerid,a.cfr_date,a.cfr_month,a.surplusqty,
        |ifnull(b.surplusqty,0) nowqty,ifnull(c.surplusqty,0) lastqty,
        |d.`销售渠道名称` as channel_name,ifnull(e.`名称`,'其他') as suppliername
        |from  sale_fact_a0  a
        |
        |left join stock_a1 b
        |on a.goodsid=b.goodsid
        |and a.nowmonth=b.cfr_month
        |
        |LEFT JOIN stock_a1 c
        |on a.goodsid=c.goodsid
        |and a.lastmonth=c.cfr_month
        |
        |left join supp_tmp d
        |on a.customerid = d.ID
        |
        |left join supply_tmp e
        |on a.goodsid =  e.goodsid
        |and a.batch = e.batch
        |and a.lots = e.lots
        |where a.saletype in ('采购入库','采退出库')
      """.stripMargin)
  val sale_fact_a5=sale_fact_a2.union(sale_fact_a4)

   //供应商表 //客户表
  /* val  Supplier=spark.sql("select id ,`名称`,`企业类型` from supp_tmp")
    val  Customer=spark.sql("select id ,`名称`,`企业类型` from cust_tmp")
    val DF1=Supplier.union(Customer)
    DF1.createOrReplaceTempView("CUSTOMER")
    val  DFCustomer=spark.sql("select id customerid,`名称` as customername,`企业类型` as customertype from CUSTOMER")*/
  val  DFCustomer=spark.sql("select id customerid,`名称` as customername,`企业类型` as customertype from supp_tmp union select id customerid,`名称` as customername,`企业类型` as customertype from cust_tmp")
   // DFCustomer.show()
    //换算率表
    val changel_a1=spark.sql("select a.id,a.`名称`,a.`单位`,b.`换算单位`,MAX(b.`换算率`) as hl from goods a " +
      "left join changes b on a.id=b.id and a.`单位`=b.`换算单位` group by a.id,a.`名称`,a.`单位`,b.`换算单位`")
   // changel_a1.show()
    changel_a1.createOrReplaceTempView("changel_a1")
   val changelDF=spark.sql( "select a.id id,a.`单位` as goodsunit,first(b.`单位`) as bigunit,a.hl as changel from changel_a1 a " +
      "left join changes b on a.id=b.id and a.`换算单位`=b.`换算单位` and a.hl=b.`换算率` group by a.id,a.`单位`,b.`换算单位`,a.hl")
    changelDF.createOrReplaceTempView("changelDF")
    //商品表
    val goods_df=spark.sql("select a.ID goodsid,a.`名称` as goodsname,a.`规格` goodsspetype,a.`单位` as goodsunit,b.bigunit,b.changel ,a.`生产厂商` vendorname," +
      "a.`批准文号` approvedno,a.`商品类型` goodstype from goods a left join changelDF b on a.id=b.id ")




    //写入销售事实表
     spark.sql("use xlz_data")
    sale_fact_a5.repartition(1).write.mode("overwrite").option("timestampFormat","yyyy-MM-dd").orc(s"hdfs://nameservice1/user/hive/warehouse/xlz_data.db/ods_sale_fact/dt="+dt+"")
     spark.sql(s"alter table ods_sale_fact add if not exists partition(dt="+dt+")")
     //写入客户表
     DFCustomer.write.mode("overwrite").option("timestampFormat","yyyy-MM-dd").orc(s"hdfs://nameservice1/user/hive/warehouse/xlz_data.db/d_customer")
     //写入商品表
     goods_df.repartition(1).write.mode("overwrite").option("timestampFormat","yyyy-MM-dd").orc("hdfs://nameservice1/user/hive/warehouse/xlz_data.db/d_goods")

    /*
     //销售信息表
     spark.sql(
       """
         |select (`商品id` + `库房ID` + `批号` + `批次`) as rowkey ,
         |`商品id` goodsid,`库房ID` warehouseid,(`数量`) surplusqty,
         |(`金额`) money,`批号` lots,`批次` batch,
         |`单价`,`成本金额`,`含税金额`,`生产日期`,`业务员ID`,`结余数量`,`结余金额`,`商户id`,
         |(`有效期至`) latestdate,(`日期`) cfr_date,
         |(`日期`) cfr_month,b.*
         |from storeDtl a
         |left join cust_tmp b
         |on a.`商户ID` = b.ID
         |where a.`业务类型` in
         |('销退验入','销售出库')
       """.stripMargin).show()
 */

  storeDtlDF.unpersist()
  }


}
