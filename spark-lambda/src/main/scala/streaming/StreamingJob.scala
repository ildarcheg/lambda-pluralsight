package streaming

import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import utils.SparkUtils._


object StreamingJob {
  def main(args: Array[String]): Unit = {
    // setup spark context
    val sc = getSparkContext("Lambda with Spark")
    sc.setLogLevel("WARN")

    val batchDuration = Seconds(4)

    def streamingApp(sc: SparkContext, batchDuration: Duration) = {
      val ssc = new StreamingContext(sc, batchDuration)

      val inputPath = "file:///vagrant/input"
      //    val inputPath = isIDE match {
      //      case true => "file:///vagrant/input"
      //      case false => "file:///vagrant/input"
      //    }

      val textDStream = ssc.textFileStream(inputPath)
      textDStream.print()

      ssc
    }

    val ssc = getStreamingContext(streamingApp, sc, batchDuration)

    ssc.start()
    ssc.awaitTermination()

  }

}