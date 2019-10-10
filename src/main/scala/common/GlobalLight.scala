package common

import org.apache.log4j.{Level, Logger}

/**
  * @Author: Shi Yu
  * @Date: 2019/9/6 10:00
  * @Version 1.0
  */
object GlobalLight {
  val LOG =  Logger.getLogger("org").setLevel(Level.ERROR)
  val HOST = "10.1.24.213" //10.1.24.213 bd-05
  val PORT = 9200
  val HTTP = "HTTP"
  val MODEL_LOCAL = "local[*]"// yarn local[*]

}
