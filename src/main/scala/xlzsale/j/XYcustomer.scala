package xlzsale.j


  import java.util.Properties

  import org.apache.spark.sql.SparkSession

object XYcustomer {
  def main(args: Array[String]) {
    val  spark= SparkSession.builder().master("local[2]").enableHiveSupport().appName("read hive to ddd").getOrCreate()
    //230,本地，以及208的mysql的链接URL
    val urllocal="jdbc:mysql://10.1.24.208:3306/aaa?"
    val prop3 =new Properties()
    prop3.setProperty("user","root")
    prop3.setProperty("password","bicon@123")


    spark.sql("use hiveonhbase")
    val df=spark.sql("select a.id,a.name as customname,b.name as customtype,a.registeraddr as address,a.legalperson contactname,a.contact phone from xy_bs_customer a left join xy_bs_customclass b on a.customertype=b.id")
    df.write.mode("overwrite").jdbc(urllocal,"xy_customer20190814",prop3)

  }
}
