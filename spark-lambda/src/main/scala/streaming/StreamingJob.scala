package streaming

import org.apache.spark.streaming._
import utils.SparkUtils._


object StreamingJob {
  def main(args: Array[String]): Unit = {
    // setup spark context
    val sc = getSparkContext("Lambda with Spark")


    val batchDuration = Seconds(4)
    val ssc = new StreamingContext(sc, batchDuration)

    val inputPath = "file:///vagrant/input"
//    val inputPath = isIDE match {
//      case true => "file:///vagrant/input"
//      case false => "file:///vagrant/input"
//    }

    val textDStream = ssc.textFileStream(inputPath)
    textDStream.print()

    ssc.start()
    ssc.awaitTermination()

  }

}