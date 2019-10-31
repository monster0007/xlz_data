package xlzsale.s

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ListBuffer

object XySelfGoodsStreamCustomer {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().master("local[2]").appName("RxToHive").getOrCreate()
    val sc = sparkSession.sparkContext
   // sc.setLogLevel("error")
/*sparkSession.udf.register("concat",(str1:String,str2:String)=>{
  str1.concat(str2)
})*/
    val tableList = ListBuffer("XH:D_WHL_REPORT")



    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "10.1.24.197,10.1.24.198,10.1.24.199")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    val scan = new Scan()
    val proto = ProtobufUtil.toScan(scan)
    val scanToString = Base64.encodeBytes(proto.toByteArray) // 将 scan 类转化为 String 类
    conf.set(TableInputFormat.SCAN, scanToString)

    var list = ListBuffer[RDD[(ImmutableBytesWritable, Result)]]()
    tableList.map(tableName => {
      conf.set(TableInputFormat.INPUT_TABLE, tableName)
      val tableRdd = sc.newAPIHadoopRDD(conf,
        classOf[TableInputFormat],
        classOf[ImmutableBytesWritable],
        classOf[Result])
      list += tableRdd
    })

    val saleOutDtRdd = list(0)


    //saleoutdt 转换 DF
    val dtRdd = saleOutDtRdd.map({case (_, result) => {

      val wareid = Bytes.toString(result.getValue("cf".getBytes, "WAREID".getBytes))
      val makedate = Bytes.toString(result.getValue("cf".getBytes, "MAKEDATE".getBytes))
      val customid = Bytes.toString(result.getValue("cf".getBytes, "CUSTOMID".getBytes))
      val id = Bytes.toString(result.getValue("cf".getBytes, "ID".getBytes))
      val whlprice1 = Bytes.toString(result.getValue("cf".getBytes, "WHLPRICE".getBytes))
      val purprice1 = Bytes.toString(result.getValue("cf".getBytes, "PURPRICE".getBytes))

      val wareqty1 = Bytes.toString(result.getValue("cf".getBytes, "WAREQTY".getBytes))
      val sdate= if (makedate==null){
        ""
      }else{
        makedate.substring(0,10).replace("-","")
      }
      //转换空的价格为0
      val whlprice=if(whlprice1==null){
        0
      }else{
        whlprice1.toDouble
      }
      val purprice=if(purprice1==null){0}else{purprice1.toDouble}
      val wareqty=if(wareqty1==null){0}else{wareqty1.toDouble.toInt}
      val goodsid ="2000"+wareid
      val customerid="2000"+customid

      val rowkey=sdate+id+"2000"
      val accountid="2000"
      val jobdtlstatus="1"
      val amount=if (whlprice==null){
       0
      }else{
        whlprice * wareqty
      }
      val taxmoney=(whlprice -purprice) *wareqty
      val checkgrossprofit=(whlprice-purprice)* wareqty
      Row(wareqty,sdate,goodsid,customerid,rowkey,accountid,jobdtlstatus,amount,taxmoney,checkgrossprofit)
    }
    })
    val dtSchema = StructType(
      Array(
        StructField("wareqty", IntegerType, true),
        StructField("sdate", StringType, true),
        StructField("goodsid", StringType, true),
        StructField("customerid", StringType, true),
        StructField("rowkey", StringType, true),
        StructField("accountid", StringType, true),
        StructField("jobdtlstatus", StringType, true),
        StructField("amount", DoubleType, true),
        StructField("taxmoney", DoubleType, true),
        StructField("checkgrossprofit", DoubleType, true)
      )
    )

    val saleOutdt = sparkSession.createDataFrame(dtRdd,dtSchema)
    saleOutdt.where("sdate !=''").show()
    saleOutdt.createTempView("saleoutdt")

 /*   //wareqty,wareid,makedate,customid

    val resDf = sparkSession.sql("select id,whlprice,purprice,wareqty,wareid,makedate,customid from saleoutdt")
//    println(resDf.count())
    val resRdd = resDf.rdd
    val finalRdd = resRdd.map(x=>{
      val ROW =
      val accountid = "7000"
      val sdate = x(8)
      val goodsid = accountid+x(5)
      val customerid = accountid+x(9)
      val productnumber = x(6).toString.toDouble
      val amount = x(3).toString.toDouble
      val taxmoney = x(7).toString.toDouble
      val checkgrossprofit = x(4).toString.toDouble
      val pardate = x(8).toString.replace("-","").substring(0,6)
      Row(id,sdate,goodsid,customerid,accountid,productnumber,amount,taxmoney,checkgrossprofit,pardate)
    })

    accountid
    amount
      checkgrossprofit
    customerid
    goodsid
    jobdtlstatus
    productnumber
    sdate
    taxmoney


    val finlSchema = StructType(
      Array(
        StructField("id", StringType, true),
        StructField("sdate", StringType, true),
        StructField("goodsid", StringType, true),
        StructField("customerid", StringType, true),
        StructField("accountid", StringType, true),
        StructField("productnumber", DoubleType, true),
        StructField("amount", DoubleType, true),
        StructField("taxmoney", DoubleType, true),
        StructField("checkgrossprofit", DoubleType, true),
        StructField("pardate", StringType, true)
      )
    )
    val finalDf=sparkSession.createDataFrame(finalRdd,finlSchema)
    val mysqlDatabase="sys_customer"
    val customerRelTab="pcd_custom_relationship_dw"
    val goodsRelTab="pcd_goods_rel"
    val username="root"
    val passwd="123456"
    val customerRelDf = getFromMysql(sparkSession,mysqlDatabase,customerRelTab,username,passwd)
    customerRelDf.createTempView("customerRel")
    val goodsRelDf=getFromMysql(sparkSession,mysqlDatabase,goodsRelTab,username,passwd)
    goodsRelDf.createTempView("goodsRel")
    finalDf.createTempView("final_table")
//    finalDf.show()
//    goodsRelDf.show()
    val dat=getYesterdayMonthHive()
    val res = sparkSession.sql("select t1.id,t1.sdate,t3.ownproduct_id goodsid,IFNULL(t2.pcd_id,0) customerid,t1.accountid,t1.productnumber*t3.quantity productnumber,t1.amount,case when t1.amount/t1.productnumber*t3.quantity<1 then 1 else 0 end as is_gift from final_table t1 left join customerRel t2 on t1.customerid=t2.customerid  join goodsRel t3 on t1.goodsid=t3.bus_goodsid where t1.pardate ="+"\""+ dat +"\"")

   // val path="hdfs://nameservice1/user/hive/warehouse/sys_customer.db/dw_sale_fact/pardate="+dat
//    println(res.count())
//    res.show(1000)
      //  res.repartition(1).write.mode("append").option("timestampFormat", "yyyy/MM/dd HH:mm:ss").csv(path)
  }

 /* .udf.register("concat",(str1:String,str2:String)=>{
    str1.concat(str2)
  })*/

  def getYesterdayMonthHive():String= {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMM")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    var yesterday = dateFormat.format(cal.getTime())
    yesterday
  }
  def getFromMysql(spark:SparkSession,databaseName:String,tableName:String,userName:String,password:String): DataFrame ={
    val jdbcDF = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://10.1.24.209:3306/" + databaseName)
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", tableName)
      .option("user", userName)
      .option("password", password).load()
    jdbcDF
  }*/

}}
