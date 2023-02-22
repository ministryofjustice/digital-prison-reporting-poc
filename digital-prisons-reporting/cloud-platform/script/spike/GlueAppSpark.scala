import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.{GlueArgParser, Job, JsonOptions}
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._
import org.apache.spark.sql.streaming.Trigger

/**
 * Attempt at creating a job to route Kinesis records as follows
 *
 *          Kinesis
 *         / (load)
 *  Kinesis (DMS events)
 *         \ (insert, update, delete)
 *          Kinesis
 *
 * This implementation runs into issues with protobuf dependencies. AWS Glue adds 4 different versions to the class
 * path which seems to break compatibility with the spark-sql-kinesis library which depends on a specific protobuf
 * version when setting up the Kinesis producer. (Despite shading the required version this still fails).
 *
 * Retained for future reference.
 */
object GlueAppSpark {
  def main(sysArgs: Array[String]) {
    val spark: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(spark)



    // @params: [JOB_NAME]
    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME").toArray)
    Job.init(args("JOB_NAME"), glueContext, args.asJava)

    println("Job started")

    val sourceStreamSettings =
      """
        |{
        |   "typeOfData": "kinesis",
        |   "streamARN": "arn:aws:kinesis:eu-west-2:771283872747:stream/dpr-kinesis-partitioned-ingestor-spike",
        |   "classification": "json",
        |   "startingPosition": "earliest",
        |   "inferSchema": "true"
        |}
        |""".stripMargin

    val dataFrame = glueContext.getSource(
      connectionType = "kinesis",
      connectionOptions = JsonOptions(sourceStreamSettings),
      transformationContext = "dataframe_AmazonKinesis_node1676900885630"
    ).getDataFrame()

    dataFrame.printSchema()

    dataFrame
      .toDF("data")
      .selectExpr("CAST(rand() AS STRING) as partitionKey", "data")
      .writeStream
      .foreachBatch({ (batchDF: DataFrame, batchId: Long) =>
        println(s"Processing batchID: $batchId")
        println(s"Batch schema")
        batchDF.printSchema()
        batchDF.show()
      })
      .format("kinesis")
      .outputMode("update")
      .option("streamName", "dpr-kinesis-partitioned-load-events-spike")
      .option("endpointUrl", "https://kinesis.eu-west-2.amazonaws.com")
      .trigger(Trigger.ProcessingTime("100 seconds"))
      .option("checkpointLocation", s"${args("TempDir")}/${args("JOB_NAME")}/checkpoint/")
      .start()
      .awaitTermination()

    println(s"Job completed")

    Job.commit()
  }
}