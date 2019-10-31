/*
import com.google.common.eventbus.Subscribe
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hbase.thirdparty.com.google.common.eventbus.Subscribe
import org.apache.htrace.fasterxml.jackson.databind.deser.std.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.spark_project.guava.eventbus.Subscribe

import scala.util.parsing.json.JSON
/**
  * Editor:         idea 
  * Department:     BigData Group
  * Author:         Hao Cheng
  * Data:           2019/8/9  9:47
  * Description: 
  *
  */
object sparkStreaming {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("yarn").appName("xy analysis kafka to hbase").getOrCreate()
    val sc = sparkSession.sparkContext
    //    sc.setLogLevel("error")
    val ssc = new StreamingContext(sc,Seconds(30))
    val brokers = "10.1.24.230:9092"
    //    val brokers = "192.168.27.11:9092"
    val kafkaParams = scala.collection.immutable.Map[String,Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
//      "group.id" -> "xydata_to_hbase_4",
//            "group.id" -> "xydata_to_hbase_sytest3_l",
            "group.id" -> "chtest6",
      "auto.offset.reset" -> "earliest",//earliest,latest
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )

    //    val path="hdfs://nameservice1/user/config/analysis_kafka_hbase/"
//    val path="hdfs://nameservice1/user/config/analysis_kafka_hbase/topic.txt"
//    val whiteTables=sparkSession.sparkContext.textFile(path).collect()
        val topic=Array("xy.public.zy_mapping")
//    val topic=whiteTables

    val kafkaDirectStream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topic, kafkaParams)).map(sourceData => {
      //      println(sourceData.value())
      println("offset : " + sourceData.offset())
      var hbasename:String=""
      var after:Map[String,Any]=Map()
      var source :Map[String, Any]=null
      var op:String=""
      var key:String=""
      try {
        try {
          val jsonkey = JSON.parseFull(sourceData.key()).get.asInstanceOf[Map[String, Any]]
          jsonkey.keySet.toList.sorted.foreach(x => {
            if (key == "") {
              key = jsonkey.get(x).get.toString
            } else {
              key = key + "|" + jsonkey.get(x).get.toString
            }
          })
        }catch {
          case e:NullPointerException=>key=""
        }

        val jsonObjmap= JSON.parseFull(sourceData.value()).get.asInstanceOf[Map[String, Any]]
        source=jsonObjmap.get("source").get.asInstanceOf[Map[String, Any]]
        op=jsonObjmap.get("op").get.toString
        val name=source.get("name").get.toString
        val table=source.get("table").get .toString.toLowerCase()
        if (name=="xy"){
          hbasename="biz_xy:"+table
        }
        after = jsonObjmap.get("after").get.asInstanceOf[Map[String,Any]]
      }catch {
        case  e:Exception=>{e.printStackTrace();after=Map()}
      }
      (key,after,op,hbasename)
    })

    //    kafkaDirectStream.cache

    kafkaDirectStream.foreachRDD(rdd=>{
      if (!rdd.isEmpty()){
        rdd.foreachPartition(iter=>{
          val conf = HBaseConfiguration.create()
          conf.set("hbase.zookeeper.quorum", "10.1.24.199,10.1.24.200,10.1.24.201")
          conf.set("hbase.zookeeper.property.clientPort", "2181")
          conf.set("hbase.defaults.for.version.skip", "true")
          var connection:Connection = null
          connection = ConnectionFactory.createConnection(conf)

          iter.foreach(line=> {
            val tableName: TableName = TableName.valueOf(line._4)

            if ( !(line._1).equals("")) {
              val saleTable = connection.getTable(tableName)
              val rowkey = line._1

              val put = new Put(Bytes.toBytes(rowkey))
              val op = (line._3).toLowerCase()
              if (op.equals("u") || op.equals("r") || op.equals("c")) {

                (line._2).keySet.toList.sorted.foreach(col => {
                  if (line._4 == "biz_xy:cms_su_notic" && col.toLowerCase() == "information_attachment") {

                  } else {
                    val value = try {
                      (line._2).get(col).get.toString
                    } catch {
                      case e: NullPointerException => "null"
                    }
                    put.addColumn("cf".getBytes("utf-8"), col.toLowerCase().getBytes("utf-8"), value.getBytes("utf-8"))
                  }
                })
                saleTable.put(put)
                println("add or updata tablename : " + tableName + " key : " + rowkey )
              } else if (op.equals("d")) {
                val delete = new Delete(Bytes.toBytes(rowkey))
                saleTable.delete(delete)
                println("delete  tablename : " + tableName + " key : " + rowkey )
              } else {
                println("other op: " + op)
              }
              saleTable.close()
            }
          })
        })
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
*/
