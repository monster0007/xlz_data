package xlzsale.s

import java.text.SimpleDateFormat
import java.util.Calendar

object LZNtest01 {
  //前一个月最后一天
  def getLastMonth(str1:String): String ={
    val dfs:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal: Calendar= Calendar.getInstance
    val ss=str1.split("-")
    cal.set(ss(0).toInt,ss(1).toInt,0)
    cal.add(Calendar.MONTH,-1)
    dfs.format(cal.getTime)
  }
  //前三个月第一天
  def getLastThreeMonth(str1:String): String ={
    val dfs:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal: Calendar= Calendar.getInstance
    val ss=str1.split("-")
    cal.set(ss(0).toInt,ss(1).toInt,1)
    cal.add(Calendar.MONTH,-4)
    dfs.format(cal.getTime)
  }
  //前45天
  def getLastDay(str1:String): String ={
    val dfs:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal: Calendar= Calendar.getInstance
    val ss=str1.split("-")
    cal.set(ss(0).toInt,ss(1).toInt,-44)
    cal.add(Calendar.MONTH,-1)
    dfs.format(cal.getTime)
  }

  def getBeforSixMonth(str1:String): String ={
    val dfs:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal: Calendar= Calendar.getInstance
    val ss=str1.split("-")
    cal.set(ss(0).toInt,ss(1).toInt,ss(2).toInt)
    cal.add(Calendar.MONTH,5)
    dfs.format(cal.getTime)
  }
  def main(args: Array[String]): Unit = {
   // println(getLastMonth("2019-10-10"))
    //println(getLastThreeMonth("2019-10-10"))
   println(getBeforSixMonth("2019-10-09"))

  }
}
