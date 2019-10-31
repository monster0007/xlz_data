package xlzsale.j

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object XlzWriteMysql {
  val day="2019-10-20"
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]").appName("").enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("error")
    val url230="jdbc:mysql://10.1.24.230:3306/biz_lzn?"
    val url209="jdbc:mysql://10.1.24.209:3306/storestream2?useUnicode=false&characterEncoding=gbk"
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

    XlzStoreWareApp(spark, url230,url209, prop, proper,day)

  }

  //今天的日期
  def getNowDay(): String ={
    val dfs:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal: Calendar= Calendar.getInstance
    dfs.format(cal.getTime)
  }
  //前一个月最后一天
  def getLastMonthDay(str1:String): String ={
    val dfs:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal: Calendar= Calendar.getInstance
    val ss=str1.split("-")
    cal.set(ss(0).toInt,ss(1).toInt,0)
    cal.add(Calendar.MONTH,-1)
    dfs.format(cal.getTime)
  }
  //前三个月第一天
  def getFirstThreeMonthDay(str1:String): String ={
    val dfs:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal: Calendar= Calendar.getInstance
    val ss=str1.split("-")
    cal.set(ss(0).toInt,ss(1).toInt,1)
    cal.add(Calendar.MONTH,-4)
    dfs.format(cal.getTime)
  }
  //前45天
  def getUnsaleDay(str1:String): String ={
    val dfs:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal: Calendar= Calendar.getInstance
    val ss=str1.split("-")
    cal.set(ss(0).toInt,ss(1).toInt,-44)
    cal.add(Calendar.MONTH,-1)
    dfs.format(cal.getTime)
  }
  //后6个月
  def getBeforSixMonth(str1:String): String ={
    val dfs:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal: Calendar= Calendar.getInstance
    val ss=str1.split("-")
    cal.set(ss(0).toInt,ss(1).toInt,ss(2).toInt)
    cal.add(Calendar.MONTH,5)
    dfs.format(cal.getTime)
  }


//b.`商品id`, b.`批次`,b.`批号`,b.`商户ID`,b.`日期`
  private def XlzStoreWareApp(spark: SparkSession, url230: String,url209: String, prop: Properties, proper: Properties,dayyy: String) = {
    //获取相关的加工基础表

    val stock = spark.read.jdbc(url230, "( select * from `结账_库存日报` where  `日期` >'2019-01-01') t", prop)
    val sale_fact = spark.read.jdbc(url230, "(select * from 库存_库房商品流水_copy3  where  `日期` >'2019-01-01') t", prop)
    val supplier = spark.read.jdbc(url230, "(select ID,`名称`,`企业类型` from 基础_供应商信息 ) t", prop)
    val customer = spark.read.jdbc(url230,"(select ID,`名称`,`企业类型` from 基础_客商信息 ) t",prop)
    val goods = spark.read.jdbc(url230, "(select * from 基础_商品信息) t", prop)
    stock.createOrReplaceTempView("stock")
    sale_fact.createOrReplaceTempView("sale_fact")
    supplier.createOrReplaceTempView("supplier")
    customer.createOrReplaceTempView("customer")
    goods.createOrReplaceTempView("goods")

    spark.udf.register("sub", (str: String) => {
      str.trim.substring(0, 10)
    })

    val storeFactDF = spark.sql("select * from stock where  `日期` in (select max(`日期`) from stock) ")
    storeFactDF.createOrReplaceTempView("store")

    //处理供应商信息
    /*
    val saleFactDF = spark.read.jdbc(url230, "(select b.* from sale_fact b where  b.`业务类型`='采购入库' ) t", prop)
    saleFactDF.createOrReplaceTempView("sale")
  val storefact_a1= spark.sql(
      """
        |select a.`商品id`, a.`批次`,a.`批号`,a.`有效期至`,a.`数量`,c.`名称`
        |from store a
        |left join sale b
        |on a.`批号` = b.`批号`
        |and a.`批次` = b.`批次`
        |and a.`商品id` = b.`商品id`
        |left join supplier c
        |on b.`商户ID`=c.id
      """.stripMargin)

    storefact_a1.createOrReplaceTempView("storefact")

    // 获取在库商品完整信息
    val resultDF = spark.sql(
      """
        |select a.`商品id` as goodsid,b.`名称` as goodsname,b.`规格` as goods_spetype,a.`批号` as lots,
        |a.`批次` as batch,sub(a.`有效期至`) as endvalidate,b.`生产厂商` as vendor,
        |b.`批准文号` as approvedno,a.`名称` as supplyer_name
        |,sum(a.`数量`) as goodsqty,b.`单位` as unit,sub(now()) as update_time
        |from storefact a
        |left join goods b
        |on a.`商品id` = b.`ID`
        |group by a.`商品id`,b.`名称`,b.`规格`,a.`批号`,
        |a.`批次`,sub(a.`有效期至`),b.`生产厂商`,
        |b.`批准文号`,a.`名称`
        |,b.`单位`,sub(now())
      """.stripMargin)
   */
  //采购库存预警
  val stock_b1 = spark.sql( s"select * from stock where  `日期`>'${getFirstThreeMonthDay(dayyy)}'")
    val changes=spark.read.jdbc(url230,"基础_商品单位换算率",prop)
    changes.createOrReplaceTempView("changes")
    stock_b1.createOrReplaceTempView("stock_b1")

       /*stock_b1.show()
            val stock_b2= spark.sql(
             s"""
               |select `商品ID` as goodsid,sum(`数量`) as qty,`日期`,`批次` as batch,`批号` as lots,`日期` as cfr_date
               |from stock_b1
               |where `日期`='$dayyy'
               |group by `商品ID`,`日期`,`批号`,`批次`
             """.stripMargin)
       stock_b2.createOrReplaceTempView("stock_b2")

       val sale_fact_a1=spark.sql(
         s"""
           |select a.`批次`,a.`批号`,a.`商品ID` as goodsid,c.`名称` goodsname,c.`规格` goodsspetype,c.`批准文号` approvedno,
           |sum(case when a.`业务类型`='采购入库' then a.`数量`  when a.`业务类型`='采退出库' then -a.`数量` else 0  end  )/3  as avgl,
           |a.`商户ID`,a.`业务类型`,b.`名称` as suppliername
           |from sale_fact a
           |left join supplier b on a.`商户ID`=b.ID
           |left join goods c on a.`商品ID`=c.`ID`
           |where a.`日期`>'${getFirstThreeMonthDay(dayyy)}' and a.`日期`<'${getLastMonthDay(dayyy)}'
           |and a.`业务类型` in ('采退出库','采购入库')
           |group by a.`批次`,a.`批号`,a.`商品ID`,a.`商户ID`,a.`业务类型`,b.`名称`,c.`名称`,c.`规格`,c.`批准文号`
         """.stripMargin
         )
       sale_fact_a1.createOrReplaceTempView("sale_fact_a1")
       val sale_fact_a2=spark.sql(
         """
           |select a.goodsid,a.qty,b.avgl,a.lots,a.batch,b.suppliername,a.cfr_date,b.goodsname,b.goodsspetype,b.approvedno
           |from
           |(
           |select `商品ID` as goodsid,sum(`数量`) as qty,`日期`,`批次` as batch,`批号` as lots,`日期` as cfr_date
           |from stock_b1
           |where `日期`='$dayyy'
           |group by `商品ID`,`日期`,`批号`,`批次`
           |) a
           |left join
           |(
           |select a.`批次`,a.`批号`,a.`商品ID` as goodsid,c.`名称` goodsname,c.`规格` goodsspetype,c.`批准文号` approvedno,
           |sum(case when a.`业务类型`='采购入库' then a.`数量`  when a.`业务类型`='采退出库' then -a.`数量` else 0  end  )/3  as avgl,
           |a.`商户ID`,a.`业务类型`,b.`名称` as suppliername
           |from sale_fact a
           |left join supplier b on a.`商户ID`=b.ID
           |left join goods c on a.`商品ID`=c.`ID`
           |where a.`日期`>'${getFirstThreeMonthDay(dayyy)}' and a.`日期`<'${getLastMonthDay(dayyy)}'
           |and a.`业务类型` in ('采退出库','采购入库')
           |group by a.`批次`,a.`批号`,a.`商品ID`,a.`商户ID`,a.`业务类型`,b.`名称`,c.`名称`,c.`规格`,c.`批准文号`
           |) b
           |on a.goodsid=b.goodsid and a.lots=b.`批号` and a.batch=b.`批次`
           |where a.qty<b.avgl order by a.goodsid
         """.stripMargin)
       sale_fact_a2.createOrReplaceTempView("sale_fact_a2")
       */
      val salewarring=spark.sql(
         s"""
           |select sum(a.qty) qty,sum(a.avgl) avgqty,a.goodsid,a.goodsname,a.goodsspetype,a.approvedno,a.suppliername,a.cfr_date,a.batch,a.lots
           |from
           |(
           |select a.goodsid,a.qty,b.avgl,a.lots,a.batch,b.suppliername,a.cfr_date,b.goodsname,b.goodsspetype,b.approvedno,a.batch,a.lots
           |from
           |(
           |select `商品ID` as goodsid,sum(`数量`) as qty,`日期`,`批次` as batch,`批号` as lots,`日期` as cfr_date
           |from stock_b1
           |group by `商品ID`,`日期`,`批号`,`批次`
           |) a
           |left join
           |(
           |select a.`批次`,a.`批号`,a.`商品ID` as goodsid,c.`名称` goodsname,c.`规格` goodsspetype,c.`批准文号` approvedno,
           |sum(case when a.`业务类型`='采购入库' then a.`数量`  when a.`业务类型`='采退出库' then -a.`数量` else 0  end  )/3  as avgl,
           |a.`商户ID`,a.`业务类型`,b.`名称` as suppliername
           |from sale_fact a
           |left join supplier b on a.`商户ID`=b.ID
           |left join goods c on a.`商品ID`=c.`ID`
           |where a.`日期`>'${getFirstThreeMonthDay(dayyy)}' and a.`日期`<'${getLastMonthDay(dayyy)}'
           |and a.`业务类型` in ('采退出库','采购入库')
           |group by a.`批次`,a.`批号`,a.`商品ID`,a.`商户ID`,a.`业务类型`,b.`名称`,c.`名称`,c.`规格`,c.`批准文号`
           |) b
           |on a.goodsid=b.goodsid and a.lots=b.`批号` and a.batch=b.`批次`
           |where a.qty<b.avgl order by a.goodsid
           |)  a
           |group by  a.goodsid,a.suppliername,a.cfr_date,a.batch,a.lots,a.goodsname,a.goodsspetype,a.approvedno""".stripMargin)
    salewarring.createOrReplaceTempView("salewaring_tmp")
    val tmp = spark.sql(
      """
        |select a.*,b.`换算率` changel FROM salewaring_tmp a
        |left join changes b on a.goodsid = b.ID
      """.stripMargin)
      import org.apache.spark.sql.functions._
    val xlz_store_suwaring=tmp.select(col("qty").cast(DoubleType),col("avgqty").cast(DoubleType),col("goodsid").cast(IntegerType),col("goodsname").cast(StringType),col("goodsspetype").cast(StringType),col("approvedno").cast(StringType),col("suppliername").cast(StringType),col("batch"),col("lots"),col("changel"),col("cfr_date").cast(DateType))


//在库滞销
/*
    val un_sale=spark.sql(
      s"""
        |select a.`商品ID`,b.`名称`,b.`规格`,b.`单位`,b.`商品类型`,b.`生产厂商`,b.`批准文号`,'${dayyy}' as cfr_date,
        |sum(case when c.`业务类型`='销售出库' then c.`数量`  when c.`业务类型`='销退验入' then -c.`数量` else 0  end) as saleqty
        |from stock_b1 a
        |left join goods b on a.`商品ID`=b.`ID`
        |left join sale_fact c on a.`商品ID`=c.`商品ID` and a.`批次`=c.`批次` and a.`批号`=c.`批号`
        |and a.`日期`=c.`日期` and a.`库房ID`=c.`库房ID`
        |where  c.`业务类型` in ('销售出库','销退验入')
        |and c.`日期`>'${getUnsaleDay(dayyy)}' and c.`日期`<'${dayyy}'
        |group by a.`商品ID`,b.`名称`,b.`规格`,b.`单位`,b.`商品类型`,b.`生产厂商`,b.`批准文号`
      """.stripMargin)
    un_sale.createOrReplaceTempView("un_sale")

    //group by a.`商品ID`,b.`名称`,b.`规格`,b.`商品类型`,b.生产厂商,b.`批准文号`,b.`单位`
    //sum(case when c.`业务类型`='采购入库' then c.`数量`  when c.`业务类型`='采退出库' then -c.`数量` else 0  end) as  dddd,
    val un_su=spark.sql(
      s"""
        |select a.`商品ID`,b.`名称`,b.`规格`,b.`商品类型`,b.`生产厂商`,b.`批准文号`,'${dayyy}' as cfr_date,
        |sum(case when c.`业务类型`='采购入库' then c.`数量`  when c.`业务类型`='采退出库' then -c.`数量` else 0  end) as  suqty,
        |b.`单位`
        |from stock a
        |left join goods b on a.`商品ID`=b.`ID`
        |left join sale_fact c on a.`商品ID`=c.`商品ID`
        |and a.`批次`=c.`批次` and a.`批号`=c.`批号`
        |and a.`日期`=c.`日期` and a.`库房ID`=c.`库房ID`
        |where  c.`业务类型` in ('采购入库','采退出库')
        |and c.`日期`>='${getUnsaleDay(dayyy)}' and c.`日期`<='${dayyy}'
        |group by a.`商品ID`,b.`名称`,b.`规格`,b.`商品类型`,b.`生产厂商`,b.`批准文号`,b.`单位`
      """.stripMargin)

    un_su.createOrReplaceTempView("un_su")

  val unsale =spark.sql(
      s"""
        |select a.`商品ID` goodsid,b.`名称` goodsname,b.`规格` goodsspetype,a.`单位` goodsunit,b.`生产厂商` vendorname,b.`批准文号` approvedno,a.saleqty,b.suqty*0.2 as offline,b.`商品类型` goodstype,a.cfr_date
        |from
        |(
        |select a.`商品ID`,b.`名称`,b.`规格`,b.`单位`,b.`商品类型`,b.`生产厂商`,b.`批准文号`,'${dayyy}' as cfr_date,
        |sum(case when c.`业务类型`='销售出库' then c.`数量`  when c.`业务类型`='销退验入' then -c.`数量` else 0  end) as saleqty
        |from stock_b1 a
        |left join goods b on a.`商品ID`=b.`ID`
        |left join sale_fact c on a.`商品ID`=c.`商品ID` and a.`批次`=c.`批次` and a.`批号`=c.`批号`
        |and a.`日期`=c.`日期` and a.`库房ID`=c.`库房ID`
        |where  c.`业务类型` in ('销售出库','销退验入')
        |and c.`日期`>'${getUnsaleDay(dayyy)}' and c.`日期`<'${dayyy}'
        |group by a.`商品ID`,b.`名称`,b.`规格`,b.`单位`,b.`商品类型`,b.`生产厂商`,b.`批准文号`
        |) a
        |left join
        |(
        |select a.`商品ID`,b.`名称`,b.`规格`,b.`商品类型`,b.`生产厂商`,b.`批准文号`,'${dayyy}' as cfr_date,
        |sum(case when c.`业务类型`='采购入库' then c.`数量`  when c.`业务类型`='采退出库' then -c.`数量` else 0  end) as  suqty,
        |b.`单位`
        |from stock a
        |left join goods b on a.`商品ID`=b.`ID`
        |left join sale_fact c on a.`商品ID`=c.`商品ID`
        |and a.`批次`=c.`批次` and a.`批号`=c.`批号`
        |and a.`日期`=c.`日期` and a.`库房ID`=c.`库房ID`
        |where  c.`业务类型` in ('采购入库','采退出库')
        |and c.`日期`>='${getUnsaleDay(dayyy)}' and c.`日期`<='${dayyy}'
        |group by a.`商品ID`,b.`名称`,b.`规格`,b.`商品类型`,b.`生产厂商`,b.`批准文号`,b.`单位`
        |) b
        |on a.`名称`=b.`名称` and a.`规格`=b.`规格` and a.`生产厂商`=b.`生产厂商` and a.`批准文号`=b.`批准文号`
        |where a.saleqty<b.suqty*0.2
      """.stripMargin)
  unsale.show()
    val xlz_store_unsale=unsale.select(col("goodsid").cast(IntegerType),col("goodsname").cast(StringType),col("goodsspetype").cast(StringType),col("goodsunit").cast(StringType),col("vendorname").cast(StringType),col("approvedno").cast(StringType),col("saleqty").cast(DoubleType),col("offline").cast(DoubleType),col("goodstype").cast(StringType),col("cfr_date").cast(DateType))
 */
    //6个月近效期
  /*
   val effect=spark.sql(
      s"""
       |select
       |c.`商品ID` cid,
       |a.`商品ID` goodsid,
       |b.`名称` as goodsname,
       |b.`规格` goodsspetype,
       |a.`批号` lots,
       |a.`批次` batch,
       |b.`生产厂商` vendorname,
       |b.`批准文号` approvedno,
       |ifnull(c.saleqty,0),
       |a.`数量` surqty,
       |b.`单位` goodsunit,
       |b.`商品类型` goodstype,
       |a.`日期` cfr_date,
       |a.`有效期至` latest_date,
       |c.`有效期至` alatest_date
       |from
       |(select * from  stock where `日期`='${dayyy}'
       |) a
       |left join goods b
       |on a.`商品ID`=b.`ID`
       |left join
       |(select a.`商品ID`,a.`批次`,a.`批号`,a.`有效期至`,a.`日期`,
       |sum(case when a.`业务类型`='销售出库'   then  a.`数量`  when a.`业务类型`='销退验入' then  -a.`数量` else 0  end  )  as saleqty
       |from sale_fact a where `日期`='${dayyy}'
       |and a.`业务类型` in ('销售出库','销退验入')
       |group by a.`商品ID`,a.`批次`,a.`批号`,a.`有效期至`,a.`日期`
       |) c
       |on a.`商品ID`=c.`商品ID` and a.`批次`=c.`批次` and a.`批号`=c.`批号` and a.`日期`=c.`日期`
       |where a.`日期`='${dayyy}' and a.`有效期至`<'${getBeforSixMonth(dayyy)}'

      """.stripMargin)
    val xlz_store_effect=effect.select(col("goodsid").cast(IntegerType),col("goodsname").cast(StringType),col("goodsspetype").cast(StringType),col("goodsunit").cast(StringType),col("vendorname").cast(StringType),col("approvedno").cast(StringType),col("saleqty").cast(DoubleType),col("surqty").cast(DoubleType), col("lots").cast(StringType),col("batch").cast(StringType),col("goodstype").cast(StringType),col("cfr_date").cast(DateType),col("latest_date").cast(DateType))

    */

    //xlz_store_effect.repartition(1).write.mode("overwrite").option("timestampFormat", "yyyy-MM-dd HH:mm:ss").jdbc(url209,"xlz_store_effect", proper)
    //xlz_store_unsale.repartition(1).write.mode("overwrite").option("timestampFormat", "yyyy-MM-dd HH:mm:ss").jdbc(url209,"xlz_store_unsale", proper)
    xlz_store_suwaring.repartition(1).write.mode("overwrite").option("timestampFormat", "yyyy-MM-dd HH:mm:ss").jdbc(url209,"xlz_store_suwarning", proper)
    //  resultDF.repartition(1).write.mode("overwrite").option("timestampFormat", "yyyy-MM-dd HH:mm:ss").jdbc(url209,"xlz_store_fact", proper)


  }
}
