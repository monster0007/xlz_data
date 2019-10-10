package common

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @Author: Shi Yu
  * @Date: 2019/9/10 18:46
  * @Version 1.0
  */
object Utils {
  def readMysql(sparkSession: SparkSession,sql:String):DataFrame= {
    val jdbcdf = sparkSession.read.format("jdbc")
      .option("url", "jdbc:mysql://10.1.24.209:3306/storestream?useUnicode=false&characterEncoding=utf-8")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "(" + sql + ")t").load()
    jdbcdf
  }
}
