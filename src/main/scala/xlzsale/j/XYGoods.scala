package xlzsale.j

import java.util.Properties

import org.apache.spark.sql.SparkSession


object XYGoods {

  def main(args: Array[String]) {
    val  spark= SparkSession.builder().master("local[2]").enableHiveSupport().appName("read hive to mysql").getOrCreate()
    //230,本地，以及208的mysql的链接URL
    val urllocal="jdbc:mysql://10.1.24.208:3306/aaa?"
    val prop3 =new Properties()
    prop3.setProperty("user","root")
    prop3.setProperty("password","bicon@123")

    spark.udf.register("pronull",(str:Double)=>{
      try{
        str.toInt
      }catch {
        case e:NullPointerException=>null
      }
    })

    spark.sql("use hiveonhbase")
    val df=spark.sql("select a.id,a.approvedno,a.goodsname,a.goodsspetype,b.vendor_name,c.gdtype,pronull(a.classid),a.goodsbusname,a.goodsunit,a.prodarea,a.barcode,d.formula_name,a.nt_priceflag,a.ephedrineflag,a.englishname,a.brand,a.otcflag,a.drug_purpose from  xy_bs_goods a left join xy_bs_vendor b on a.vendorid=b.id  left join xy_goods_custom c on a.classid=c.id left join xy_bs_goods_formula d on a.formulaid=d.id")
    df.write.mode("overwrite").jdbc(urllocal,"xy_goods20190814",prop3)

  }

}
