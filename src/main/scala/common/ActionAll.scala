package common

import org.apache.log4j.Logger
import xlz.saleDtl

/**
  * @Author: Shi Yu
  * @Date: 2019/9/6 13:38
  * @Version 1.0
  */
object ActionAll {
  def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis()

    //产品订单详情
    new saleDtl().write2es()

    val end = System.currentTimeMillis()
    val spend = (end - start)/1000
    Logger.getLogger("totalSpend").info(spend + "s")
  }
}
