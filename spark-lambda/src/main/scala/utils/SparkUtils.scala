package utils

import java.lang.management.ManagementFactory

import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{StreamingContext, Duration}
import org.apache.spark.{SparkContext, SparkConf}

object SparkUtils {
  val isIDE:Boolean = {
    ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")
  }
  def getSparkContext(appName: String) : SparkContext = {
    var checkpointDirectory = ""

    // get spark configuration
    val conf = new SparkConf()
      .setAppName(appName)

    // Check if running from IDE
    if (isIDE) {
      conf.setMaster("local[*]")
      checkpointDirectory = "file:///vagrant/TEMP"
    } else {
      checkpointDirectory = "hdfs://lambda-pluralsight:9000/spark/checkpoint"
    }

    // setup spark context
    val sc = SparkContext.getOrCreate(conf)
    sc.setCheckpointDir(checkpointDirectory)
    sc
  }

  def getSQLContext(sc: SparkContext) : SQLContext = {
    val sqlContext = SQLContext.getOrCreate(sc)
    sqlContext
  }
}
