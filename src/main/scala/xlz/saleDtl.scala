package xlz

import java.util
import java.util.concurrent.TimeUnit

import common.{ESUtils, GlobalLight}
import org.apache.http.HttpHost
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.indices.PutMappingRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestHighLevelClient}
import org.elasticsearch.common.xcontent.XContentType


/**
  * 产品订单详情
  * 采购模块 2-1,
  * 销售模块的 1-1 ,
  * 库存模块的2-1
  * 跳转到 相同的 产品订单详情
  */
class saleDtl {
  /***
    * 读取hive 获取df
    * @return
    */
  def readHiveGetDF () : DataFrame ={
    val spark = SparkSession
      .builder()
      .appName("readfHive")
      .master(GlobalLight.MODEL_LOCAL)
      .enableHiveSupport()
      .getOrCreate()
    spark.sql("USE xlz_data")
    val sql =
      """
        |SELECT
        |d.customername, -- 客户名称
        |a.saledtlid, -- 订单编号
        |b.goodsname, -- 产品名称
        |b.goodsspetype, -- 规格
        |a.lots, -- 批号
        |a.batch, -- 批次
        |a.goodsqty AS saleqty, -- 销售数量
        |b.goodsunit, --  单位
        |a.money/10000 AS salemoney, -- 销售金额
        |'万元'  AS moneyunity, -- 销售金额
        |a.cfr_date, -- 完成日期
        |d.customertype, -- CUSTOMERTYPE
        |b.goodstype,-- 商品类型
        |b.vendorname, -- 生产厂商
        |b.approvedno, -- 批准文号
        |c.ym
        |FROM ods_sale_fact a
        |LEFT JOIN d_goods b on a.goodsid=b.goodsid
        |LEFT JOIN d_time c on a.cfr_date=c.YMD
        |LEFT JOIN d_customer d on a.customerid=d.customerid
        |WHERE  a.saletype = '销售出库'
        |OR a.saletype = '销退验入'
      """.stripMargin
     val df = spark.sql(sql)
    df
  }

  /**
    * step2. 将df 写入 es
    */
  def write2es(): Unit = {

    val indexName = "xlz_data_sale_dtl"  //索引名称
    val indexType = "sale_dtl" //索引类型
    val start = System.currentTimeMillis()

    val df = readHiveGetDF()
    val resArry = df.foreachPartition(par => {
      if(!par.isEmpty){
        val mapf = new util.HashMap[String, Any]()
        var i = 0
        val blukRequest = new BulkRequest()
        blukRequest.timeout("10m")
        val listener = ESUtils.getBulkListener
        val client = new RestHighLevelClient(RestClient.builder(new HttpHost(GlobalLight.HOST, GlobalLight.PORT, GlobalLight.HTTP)))
        //获取皮操作处理
        val bulkprocessor =  ESUtils.getBulkprocessor(client,listener,50000)
        try {
          par.foreach(x =>{
            mapf.put("customername", x.get(x.fieldIndex("customername")))//客户名称
            mapf.put("saledtlid", x.get(x.fieldIndex("saledtlid")))//订单编号
            mapf.put("goodsname", x.get(x.fieldIndex("goodsname")))//产品名称
            mapf.put("goodsspetype", x.get(x.fieldIndex("goodsspetype")))//规格
            mapf.put("lots", x.get(x.fieldIndex("lots")))//批号
            mapf.put("batch", x.get(x.fieldIndex("batch")))//批次
            mapf.put("saleqty", x.get(x.fieldIndex("saleqty")))//销售数量
            mapf.put("goodsunit",   x.get(x.fieldIndex("goodsunit")))//单位
            mapf.put("salemoney", x.get(x.fieldIndex("salemoney")))//销售金额(万元)
            mapf.put("moneyunity", x.get(x.fieldIndex("moneyunity")))//销售金额单位(万元)
            mapf.put("cfr_date", x.get(x.fieldIndex("cfr_date")).toString())//完成日期
            mapf.put("customertype", x.get(x.fieldIndex("customertype")))//客户类型
            mapf.put("goodstype", x.get(x.fieldIndex("goodstype")))//商品类型
            mapf.put("vendorname", x.get(x.fieldIndex("vendorname")))//生产厂商
            mapf.put("approvedno", x.get(x.fieldIndex("approvedno")))//批准文号
            mapf.put("ym", x.get(x.fieldIndex("ym")))//时间
            val id = mapf.get("saledtlid").toString()
            val request = new IndexRequest(indexName,indexType)
            request.id(id)
            request.timeout("10m")
            request.source(mapf)
            bulkprocessor.add(request)
          })
          bulkprocessor.awaitClose(30L, TimeUnit.SECONDS)
          bulkprocessor.close()
          client.close()
        }catch{
          case e : Exception => e.printStackTrace()
        }finally {
          bulkprocessor.awaitClose(30L, TimeUnit.SECONDS)
          bulkprocessor.close()
          client.close()
        }
      }
    })
    val client2 = new RestHighLevelClient(RestClient.builder(new HttpHost(GlobalLight.HOST, GlobalLight.PORT, GlobalLight.HTTP)))
    try {
      //修改索引映射
      val putMappingRequest: PutMappingRequest = new PutMappingRequest(indexName)
      val json = "{\n\"properties\":{\n\"id\":{\n\"type\":\"text\",\n\"fielddata\":true\n}\n}\n}"
      putMappingRequest.source(json, XContentType.JSON)
      client2.indices.putMapping(putMappingRequest, RequestOptions.DEFAULT)
      client2.close()
      println("complete")
    }catch {
      case e : Exception => e.printStackTrace()
    }finally {
      client2.close()
    }
    val end = System.currentTimeMillis()
    val spend =  (end - start)/ 1000
    Logger.getLogger(this.getClass + "  ___ " + indexName + " ____ spend time:").info(spend + "s")
  }
}

object saleDtl{
  def main(args: Array[String]): Unit = {
    new saleDtl().write2es()
  }
}