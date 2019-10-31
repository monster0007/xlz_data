package xlzsale.j

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Editor:         idea 
  * Department:     BigData Group
  * Author:         Hao Cheng
  * Data:           2019/8/26  14:54
  * Description: 
  *
  */
class ReadHbase {
   def readhb(sparkSession: SparkSession,table:String,schema: StructType) = {
    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, table)
    conf.set("hbase.zookeeper.quorum", "10.1.24.197,10.1.24.198,10.1.24.199")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.defaults.for.version.skip", "true")
    val stuRDD = sparkSession.sparkContext.newAPIHadoopRDD(conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])
    val arr=schema.map(x=>(x.name,x.dataType))
    val rdd=stuRDD.map({case (_,result)=>
      val rowkey=Bytes.toString(result.getRow).toDouble.toInt
      val tmp=arr.map({case (colname,datatype)=>
        val colValue=Bytes.toString(result.getValue("cf".getBytes("UTF-8"),colname.getBytes("UTF-8")))
        try {
          if (colValue == null || colValue == "null" || colValue == "") {
            null
          } else if (datatype == IntegerType) {
            colValue.toDouble.toInt
          } else if (datatype == DoubleType) {
            colValue.toDouble
          } else if (datatype == DecimalType(19, 4)) {
            Decimal(colValue.toString.toDouble, 19, 4)
            //          colValue.toDouble
          } else {
            colValue
          }
        }catch {
          case e:NumberFormatException=>{null}
        }
      })
        Row(tmp:_*)
    })
   val df=sparkSession.createDataFrame(rdd,schema)
    df

  }



}
