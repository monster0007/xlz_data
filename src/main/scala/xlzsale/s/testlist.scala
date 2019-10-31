package xlzsale.s

import scala.collection.mutable.ArrayBuffer

object testlist {
  def main(args: Array[String]): Unit = {

    var un = Map("ss" -> "dddd", "sds" -> "厦门")

 un += ("dsd"->"sdd")
    for (ddd<-un)println(ddd)
 val filterun=un filter(kv=>kv._2 contains("d"))
    println(filterun)
filterun.foreach(kv=>println(kv._1,kv._2))

  }
}