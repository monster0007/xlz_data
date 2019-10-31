package xlzsale.s

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import org.apache.spark.sql.SparkSession

import scala.language.postfixOps

object LZNnew02 {

  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("lzn").master("local").enableHiveSupport().getOrCreate()
    //添加隐式方法，（不用schema就可以将list转为DF）
    import spark.implicits._
    val url230="jdbc:mysql://10.1.24.230:3306/biz_lzn?"
    //三个库的登录用户密码
    //230的
    val prop =new Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","bicon@123")
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    //读取230百川的三张需要用到的表
    val goods=spark.read.jdbc(url230,"基础_商品信息",prop)
    val changel=spark.read.jdbc(url230,"xlz_goodschange",prop)
    //230读取表的临时表
    goods.createOrReplaceTempView("goods")
    changel.createOrReplaceTempView("changel")


    val goods_df=spark.sql("select a.ID goodsid,a.`名称` as goodsname,a.`规格` goodsspetype,a.`单位` goodsunit,b.bigunit,b.changel ,a.`生产厂商` vendorname,a.`批准文号` approvedno,a.`商品类型` goodstype from goods a left join changel b on a.id=b.id ")
    goods_df.show()


    // 导入表到hive
     spark.sql("use xlz_data ")
   // sale_fact_a2.repartition(1).write.mode("overwrite").option("timestampFormat","yyyy-MM-dd").orc("hdfs://nameservice1/user/hive/warehouse/aftest.db/w_lzn_sale_fact_three")
    // stock_a1.repartition(1).write.mode("overwrite").option("timestampFormat","yyyy-MM-dd").orc("hdfs://nameservice1/user/hive/warehouse/aftest.db/w_lzn_stock_three")
    goods_df.repartition(1).write.mode("overwrite").option("timestampFormat","yyyy-MM-dd").orc("hdfs://nameservice1/user/hive/warehouse/xlz_data.db/d_goods")











  }
}
