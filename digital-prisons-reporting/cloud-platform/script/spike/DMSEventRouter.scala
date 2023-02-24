package gluejob

import com.amazonaws.auth.InstanceProfileCredentialsProvider
import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.{GlueArgParser, Job, JsonOptions}
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.services.kinesis.model.{PutRecordRequest, PutRecordsRequest, PutRecordsRequestEntry}
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

    // TODO - can we express the schema of the incoming data correctly?
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
        println(s"Batch: $batchId Processing ${dataFrame.count()} records")
        val startTime = System.currentTimeMillis()

        val putRequests = dataFrame.collectAsList().asScala.map { row =>
          val data = row.getString(0)

          val parsed: Map[String, Map[String, String]] = reader.readValue(data)

          val metadata = parsed.get("metadata")

          val partitionKey = metadata.flatMap(_.get("partitionKey")).getOrElse(UUID.randomUUID().toString)

          metadata.flatMap(_.get("operation")) match {
            case Some("load") => (loadEventStream, putRequestEntry(partitionKey, data))
            case _ => (cdcEventStream, putRequestEntry(partitionKey, data))
          }
        }

        // TODO - consider factoring this out
        val loadRequests = putRequests.filter(_._1 == loadEventStream).map(_._2)
        val cdcRequests = putRequests.filter(_._1 == cdcEventStream).map(_._2)

        val maxBatchSize = 500

        loadRequests.grouped(maxBatchSize).foreach { g =>
          val loadRecordsRequest = new PutRecordsRequest
          loadRecordsRequest.setRecords(g.asJava)
          loadRecordsRequest.setStreamName(loadEventStream)
          kinesisClient.putRecords(loadRecordsRequest)
        }

        cdcRequests.grouped(maxBatchSize).foreach { g =>
          val cdcRecordsRequest = new PutRecordsRequest
          cdcRecordsRequest.setRecords(g.asJava)
          cdcRecordsRequest.setStreamName(cdcEventStream)
          kinesisClient.putRecords(cdcRecordsRequest)
        }

        val runTime = System.currentTimeMillis() - startTime
        println(s"Batch: $batchId Processed ${dataFrame.count} records in ${runTime}ms - ${BigDecimal(runTime.toDouble / dataFrame.count).setScale(3, RoundingMode.HALF_UP)}ms per record")
      }
    }, JsonOptions(s"""{"windowSize" : "5 seconds", "checkpointLocation" : "${args("TempDir")}/${args("JOB_NAME")}/checkpoint/"}"""))
    Job.commit()
  }

  // TODO - handle publish failures and rework to batch to minimise number of kinesis client calls
  def putRequestEntry(partitionKey: String, data: String) = {
    val entry = new PutRecordsRequestEntry
    entry.setData(ByteBuffer.wrap(data.getBytes))
    entry.setPartitionKey(partitionKey)
    entry
  }

}