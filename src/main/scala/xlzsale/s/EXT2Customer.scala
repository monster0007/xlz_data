//import java.lang
//import java.util.Properties
//
//import oracle.net.aso.s
//import org.apache.spark.SparkContext
//import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
//import org.apache.spark.sql.{DataFrame, Row, SparkSession}
//
///**
//  * Created by Administrator on 2019/6/17 0017.
//  */
//
//object EXT2Customer {
//
//  def main(args: Array[String]) {
//
//  val  spark= SparkSession.builder().master("local[*]").enableHiveSupport().config("spark.sql.crossJoin.enabled","true").appName("localss").getOrCreate()
//   val sc=spark.sparkContext.uiWebUrl
//    //230,本地，以及208的mysql的链接URL
//    val url230="jdbc:mysql://10.1.24.230:3306/"
//    //val url218="jdbc:oracle:thin:@10.1.24.218:1521:sjhterp?"
//    val url208="jdbc:mysql://10.1.24.208:3306/bicon_pcd?"
//    val urllocal="jdbc:mysql://10.1.24.208:3306/automation?"
//    //三个库的登录用户密码
//    //230的
//    val prop =new Properties()
//    prop.setProperty("user","root")
//    prop.setProperty("password","bicon@123")
//    //208的
//    val prop1 =new Properties()
//    prop1.setProperty("user","root")
//    prop1.setProperty("password","bicon@123")
//    //218的oracle
//    /*   val prop2 =new Properties()
//       prop2.setProperty("user","zserp")
//       prop2.setProperty("password","zserp")*/
//    //本地的
//    val prop3 =new Properties()
//    prop3.setProperty("user","root")
//    prop3.setProperty("password","root")
//
//
//
//    //230创建各自库下的表的临时表
//
//
//    //读取208最新的客户和关系表
//    val pcd_custom_relationship=spark.read.jdbc(url208,"pcd_custom_relationship_20190815",prop1)
//    val pcd_custom=spark.read.jdbc(url208,"pcd_custom_20190815",prop1)
//    //拿到处理过后的最新客户
//    val pcd_new_customer=spark.read.jdbc(urllocal,"pcd_new_customer_unoincity001",prop1)
//
//    //208读取表的临时表
//    pcd_custom_relationship.createOrReplaceTempView("pcd_custom_relationship")
//    pcd_custom.createOrReplaceTempView("pcd_custom")
//    pcd_new_customer.createOrReplaceTempView("pcd_new_customer")
//
//    //处理最新客户（去重）
//    val no_repeat_group=spark.sql("select min(id) as id from pcd_new_customer group by customname,province,city")
//    no_repeat_group.createOrReplaceTempView("no_repeat_group")
//   val no_repeat_group1=spark.sql("select * from pcd_new_customer a where exists(select id from no_repeat_group b where a.id=b.id)")
//   //自身未重复的
//    no_repeat_group1.createOrReplaceTempView("no_repeat")
//
//    //先从自身筛出重复的（新的自身重复的）
//    val repeat=spark.sql("select * from pcd_new_customer a where not exists(select id from  no_repeat b where a.id=b.id)")
//    repeat.createOrReplaceTempView("repeat")
//    //用自身未重的跟主数据比较，得到重复的（新的里面，与主数据重复的）
//    val repeat2=spark.sql("select * from no_repeat a where exists(select id from pcd_custom b where a.customname=b.customname and a.province=b.province and a.city=b.city)")
//    //用自身未重的跟主数据比较，得到不重复的（完全新）
//    val no_repeat2=spark.sql("select * from no_repeat a where not exists(select id from pcd_custom b where a.customname=b.customname and a.province=b.province and a.city=b.city)")
//
// /*  repeat.write.mode("overwrite").jdbc(urllocal,"pcd_new_customer_新增主数据自重",prop1)
//   repeat2.write.mode("overwrite").jdbc(urllocal,"pcd_new_customer_新增主数据重",prop1)
//   no_repeat2.write.mode("overwrite").jdbc(urllocal,"pcd_new_customer_新增主数据未重",prop1)*/
//  repeat.union(repeat2).createOrReplaceTempView("pcd_customer_repeat")
//    no_repeat2.createOrReplaceTempView("pcd_customer_no_repeat")
//    val new_cus=spark.sql("select customname,customtype,address,province,city,contactname,phone,id as customid,'0' as isexcel from pcd_customer_no_repeat ")
//    //new_cus.show()
//    //查询客户表已有的最大ID
//
//
//   /* val SR=spark.sql("select max(id)as id from pcd_custom ")
//   val S=SR.foreach(F=>{
//    F.getString(1)
//   })*/
//
//
//
//    //添加自增序列ID
//    def addId(df:DataFrame):DataFrame= {
//      val rows = df.rdd.zipWithIndex().map {
//        case (r: Row, id: Long) => Row.fromSeq(id +: r.toSeq)
//      }
//      val s = df.schema
//      // 再由 row 根据原表头进行转换
//      val dfWithPK = spark.createDataFrame(rows, StructType(StructField("id", LongType, false) +: s.fields))
//      dfWithPK
//    }
//   // addId(new_cus).show()
//    val add_id=addId(new_cus)
//   // add_id.write.mode("overwrite").jdbc(urllocal,"pcd_new_customer_新增主数据1",prop1)
//    add_id.createOrReplaceTempView("add_id")
//    val add_id2=spark.sql("select id,customname,customtype,address,province,city,contactname,phone,customid,isexcel from add_id order by id")
//    add_id2.write.mode("overwrite").jdbc(urllocal,"pcd_new_customer_新增主数据未重",prop1)
//   val add_id3=spark.sql("select * from pcd_customer_repeat")
//   add_id3.write.mode("overwrite").jdbc(urllocal,"pcd_new_customer_新增主数据重",prop1)
//    /* sss=spark.sql("select * from pcd_custom")
//    sss.write.mode("overwrite").jdbc(urllocal,"pcd__customer",prop)
//    no_repeat.write.mode("overwrite").jdbc(urllocal,"pcd__customer111",prop)
//*/
//    /*val rdd=spark.sparkContext.parallelize(Seq(
//      Row("ss","ff","gg"),
//      Row("dd","ff","gg")
//    ))
//    val schema=StructType(Seq(
//      StructField("c1",StringType,false),
//      StructField("c2",StringType,false),
//      StructField("c3",StringType,false)
//    ))
//    val df=spark.createDataFrame(rdd,schema)
//    df.show()
//    val schema1=df.schema.add("id",LongType,false)
//    val rdd1=df.rdd.zipWithIndex().map(x=> Row.merge(x._1,Row(x._2+10)))
//    val res=spark.createDataFrame(rdd1,schema1)
//    res.show()*/
//  }
//
//}
