package xlzsale.j

import org.apache.spark.sql.types.{DecimalType, IntegerType, StringType, StructField}
import org.apache.spark.sql.{SparkSession, types}

object TestSalehbase {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().master("local[2]").appName("RxToHive").getOrCreate()

    val readHbaseClass =new ReadHbase
    val cms_su_dtl_schema=types.StructType(Seq(
      StructField("id",IntegerType,true),
      StructField("suid",IntegerType,true),
      StructField("goodsid",IntegerType,true),
      StructField("lotid",IntegerType,true),
      StructField("lotno",StringType,true),
      StructField("warebusid",StringType,true),
      StructField("batchid",IntegerType,true),
      StructField("buyerid",IntegerType,true),
      StructField("goodsqty",IntegerType,true),
      StructField("jobdtlstatus",IntegerType,true),
      StructField("srcsudtlid",IntegerType,true),
      StructField("unitprice",DecimalType(19,4),true)
    ))
    readHbaseClass.readhb(spark,"biz_xy:cms_su_dtl",cms_su_dtl_schema).createOrReplaceTempView("cms_su_dtl")

    val df=spark.sql("select * from cms_su_dtl ")
    df.show()
   // df.write.mode("overwrite").jdbc(urllocal,"xy_customer20190814",prop3)
  /*  df.repartition(1).write.mode("overwrite").option("timestampFormat","yyyy-MM-dd")
      .orc("hdfs://nameservice1/user/hive/warehouse/aftest.db/w_bs_sale_fact_orc")*/

  }
}

