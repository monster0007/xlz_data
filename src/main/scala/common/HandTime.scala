package common

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SparkSession

/**
  * Editor:         idea 
  * Department:     BigData Group
  * Author:         Hao Cheng
  * Data:           2019/8/22  11:01
  * Description: 
  *
  */
class HandTime extends Serializable {
  /**
    *  从1970-1-1 后 amount 天的日期
    * @param amount
    * @return
    */
    def eraAfterDate (amount:Int):String={
      val c=Calendar.getInstance()
      c.set(1970,0,1)
      c.add(Calendar.DATE,amount)
      val dateFormat=new SimpleDateFormat("yyyy-MM-dd")
      dateFormat.format(c.getTime)
  }
  def preThreeMonth():String={
    val c=Calendar.getInstance()
    c.add(Calendar.DATE,-1)
    val dateFormat=new SimpleDateFormat("yyyy-MM-dd")
    val datastr=dateFormat.format(c.getTime)
    val arr=datastr.split("-")
    val yy=arr(0).toDouble.toInt
    val mm=arr(1).toDouble.toInt-1
    val dd=arr(2).toDouble.toInt

    c.set(yy,mm,dd)
    c.add(Calendar.MONTH,-2)
    val monFormat=new SimpleDateFormat("yyyy-MM")
    monFormat.format(c.getTime)
  }

  /**
  注册一个 udf  名称  getDate
   */
  def udf_eraAfterDate(sparkSession: SparkSession)={
    sparkSession.udf.register("getDate",(str:String)=>{
      eraAfterDate(str.toDouble.toInt).toString
    })
  }

  def getYesterdayMonth():String={
    val c=Calendar.getInstance()
    c.add(Calendar.DATE,-1)
    val dateFormat=new SimpleDateFormat("yyyy-MM")
    dateFormat.format(c.getTime)
  }
  def getYesterdayDay():String={
    val c=Calendar.getInstance()
    c.add(Calendar.DATE,-1)
    val dateFormat=new SimpleDateFormat("yyyy-MM-dd")
    dateFormat.format(c.getTime)
  }

}
