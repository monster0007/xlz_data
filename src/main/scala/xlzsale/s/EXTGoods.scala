package xlzsale.s

import java.util.Properties

import org.apache.spark.sql.SparkSession

object EXTGoods {
 /* def main(args: Array[String]) {
    val  spark= SparkSession.builder().master("local[*]").enableHiveSupport().config("spark.sql.crossJoin.enabled","true").appName("localss").getOrCreate()

    //230,本地，以及208的mysql的链接URL
    val url230="jdbc:mysql://10.1.24.230:3306/"
    //val url218="jdbc:oracle:thin:@10.1.24.218:1521:sjhterp?"
    val url208="jdbc:mysql://10.1.24.208:3306/bicon_pcd?"
    val urllocal="jdbc:mysql://10.1.24.208:3306/automation?"
    //三个库的登录用户密码
    //230的
    val prop =new Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","bicon@123")
    //208的
    val prop1 =new Properties()
    prop1.setProperty("user","root")
    prop1.setProperty("password","bicon@123")

    //本地的
    val prop3 =new Properties()
    prop3.setProperty("user","root")
    prop3.setProperty("password","root")


    //读取各自库下的表
    val customer=spark.read.jdbc(url230+"biz_rx","bs_customer",prop)
    val city=spark.read.jdbc(url230+"biz_rx","base_city",prop)
    val province=spark.read.jdbc(url230+"biz_rx","base_province",prop)
    val u_custom=spark.read.jdbc(url230+"biz_xh","u_custom",prop)
    val infowldw=spark.read.jdbc(url230+"biz_bc","infowldw",prop)
    val tb_busimate=spark.read.jdbc(url230+"biz_kl","tb_busimate",prop)
    val jcksxx=spark.read.jdbc(url230+"biz_lzn","基础_客商信息",prop)

    val province_city=spark.read.jdbc(urllocal,"province_city",prop)
    val area=spark.read.jdbc(urllocal,"area",prop)
  }*/
}
