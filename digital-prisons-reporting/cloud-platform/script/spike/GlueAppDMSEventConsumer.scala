package gluejob

import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.{GlueArgParser, Job, JsonOptions}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import java.time.Instant
import scala.collection.JavaConverters._

/**
 * Consumes events from a DMS Kinesis stream and logs the time deltas in order to get a sense of the overall latency.
 *
 * Note that the streamARN property must be set to the ARN of the relevant source stream.
 */
object GlueApp {

  def main(sysArgs: Array[String]) {

    val spark: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(spark)

    import glueContext.sparkSession.implicits._

    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME").toArray)

    println(s"Got args: ${args}")

    Job.init(args("JOB_NAME"), glueContext, args.asJava)

    val kinesisConsumerOptions =
      """{
        |"typeOfData": "kinesis",
        |"streamARN": "arn:aws:kinesis:eu-west-2:771283872747:stream/dpr-kinesis-partitioned-load-events-spike",
        |"classification": "json",
        |"startingPosition": "earliest",
        |"schema": "`data` STRING, `metadata` MAP<STRING, STRING>"
        |}""".stripMargin

    val dataFrame: DataFrame =
      glueContext
        .getSource(
          connectionType = "kinesis",
          connectionOptions = JsonOptions(kinesisConsumerOptions),
          transformationContext="dpr-partitioning-spike")
        .getDataFrame()

    glueContext.forEachBatch(dataFrame, (dataFrame: Dataset[Row], batchId: Long) => {
      if (dataFrame.count() > 0) {
        val timeDeltas = dataFrame.map { row =>
          val metadata = row.getMap[String, String](row.fieldIndex("metadata"))
          metadata
            .get("timestamp")
            .map(Instant.parse)
            .map { ts =>
              Instant.now().toEpochMilli - ts.toEpochMilli
            }
        }.collectAsList().asScala.flatten
        println(s"Batch: $batchId Records: ${dataFrame.count()} Processing Lag - min: ${timeDeltas.min}ms max: ${timeDeltas.max}ms average: ${timeDeltas.sum / timeDeltas.length}ms")
      }
    }, JsonOptions(s"""{"windowSize" : "100 seconds", "checkpointLocation" : "${args("TempDir")}/${args("JOB_NAME")}/checkpoint/"}"""))
    Job.commit()
  }
}
