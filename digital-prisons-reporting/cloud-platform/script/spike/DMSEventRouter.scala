package gluejob

import com.amazonaws.auth.InstanceProfileCredentialsProvider
import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.{GlueArgParser, Job, JsonOptions}
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.services.kinesis.model.{PutRecordsRequest, PutRecordsRequestEntry}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.UUID
import scala.collection.JavaConverters._
import scala.math.BigDecimal.RoundingMode

object GlueApp {

  val workerId = s"${InetAddress.getLocalHost.getCanonicalHostName}:${UUID.randomUUID}"
  val appName = "CdcPartitioningSpike"
  val streamName = "dpr-kinesis-partitioned-ingestor-spike"
  val region = "eu-west-2"
  val initialStreamPosition = InitialPositionInStream.LATEST
  val credentialsProvider = InstanceProfileCredentialsProvider.getInstance()
  val loadEventStream = "dpr-kinesis-partitioned-load-events-spike"
  val cdcEventStream = "dpr-kinesis-partitioned-cdc-events-spike"

  // This is the upper limited allowed by Kinesis for batched put requests.
  val maxBatchSize = 500

  // Client used for publishing messages
  val kinesisClient = AmazonKinesisClientBuilder.standard()
    .withCredentials(credentialsProvider)
    .withRegion(region)
    .build();

  // Use jackson with scala support since it's already on the classpath.
  val reader = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .readerFor(classOf[Map[String, String]])

  def main(sysArgs: Array[String]) {

    val spark: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(spark)

    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME").toArray)

    Job.init(args("JOB_NAME"), glueContext, args.asJava)

    val kinesisConsumerOptions =
      """{
        |"typeOfData": "kinesis",
        |"streamARN": "arn:aws:kinesis:eu-west-2:771283872747:stream/dpr-kinesis-partitioned-ingestor-spike",
        |"classification": "json",
        |"startingPosition": "earliest",
        |"inferSchema": true
        |}""".stripMargin

    val dataFrame: DataFrame =
      glueContext
        .getSource(
          connectionType = "kinesis",
          connectionOptions = JsonOptions(kinesisConsumerOptions),
          transformationContext = "dpr-partitioning-spike")
        .getDataFrame()

    glueContext.forEachBatch(dataFrame, (dataFrame: Dataset[Row], batchId: Long) => {
      if (dataFrame.count() > 0) {
        val startTime = System.currentTimeMillis()

        val putRequests = dataFrame.collectAsList().asScala.map { row =>
          val data: String = row.getString(0)

          val parsed: Map[String, Map[String, String]] = reader.readValue(data)

          val metadata = parsed.get("metadata")

          val partitionKey = metadata.flatMap(_.get("partitionKey")).getOrElse(UUID.randomUUID().toString)

          metadata.flatMap(_.get("operation")) match {
            case Some("load") => (loadEventStream, Some(createPutRequestEntry(partitionKey, data)))
            // Filter out create-table events for now.
            case Some("create-table") => (loadEventStream, None)
            // Treat all other event types as CDC events e.g. insert, update, delete
            case _ => (cdcEventStream, Some(createPutRequestEntry(partitionKey, data)))
          }
        }

        putRequestsForStream(loadEventStream, putRequests)
        putRequestsForStream(cdcEventStream, putRequests)

        val runTime = System.currentTimeMillis() - startTime
        val timePerRecord = BigDecimal(runTime.toDouble / dataFrame.count).setScale(3, RoundingMode.HALF_UP)

        println(s"Batch: $batchId Processed ${dataFrame.count} records in ${runTime}ms - ${timePerRecord}ms per record")
      }
    }, JsonOptions(s"""{"windowSize" : "5 seconds", "checkpointLocation" : "${args("TempDir")}/${args("JOB_NAME")}/checkpoint/"}"""))
    Job.commit()
  }

  private def createPutRequestEntry(partitionKey: String, data: String) = {
    val entry = new PutRecordsRequestEntry
    entry.setData(ByteBuffer.wrap(data.getBytes))
    entry.setPartitionKey(partitionKey)
    entry
  }

  // TODO - handle client errors
  private def putRequestsForStream(streamName: String, requests: Seq[(String, Option[PutRecordsRequestEntry])]): Unit =
    requests
      .filter(_._1 == streamName)
      .flatMap(_._2)
      .grouped(maxBatchSize)
      .foreach { batch =>
        val putRecordsRequest = new PutRecordsRequest
        putRecordsRequest.setRecords(batch.asJava)
        putRecordsRequest.setStreamName(streamName)
        kinesisClient.putRecords(putRecordsRequest)
      }

}