import com.amazonaws.auth.InstanceProfileCredentialsProvider
import com.amazonaws.services.glue.util.Job
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder
import com.amazonaws.services.kinesis.clientlibrary.interfaces.{IRecordProcessor, IRecordProcessorCheckpointer, IRecordProcessorFactory}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{InitialPositionInStream, KinesisClientLibConfiguration, ShutdownReason, Worker}
import com.amazonaws.services.kinesis.model.{PutRecordRequest, Record}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import java.net.InetAddress
import java.nio.charset.StandardCharsets
import java.util
import java.util.UUID
import scala.collection.JavaConverters._

/**
 * Job that uses the Kinesis client to read messages from a stream of DMS events and route them to two streams for the
 * following operation types respectively
 *   o load
 *   o anything else e.g. insert, update, delete
 *
 * This job runs continuously so may not be suitable for use with Glue without further modifications.
 *
 * TODOs
 *   o consider using spark/glue kinesis code to read source stream with a custom sink using the producers
 *   o batch calls to putRecord
 *   o handle errors - no error handling at all
 *   o cleanup the code - it's a *very* rough POC
 */
object GlueAppKinesisClient {

  val workerId = s"${InetAddress.getLocalHost.getCanonicalHostName}:${UUID.randomUUID}"
  val appName = "CdcPartitioningSpike"
  val streamName = "dpr-kinesis-partitioned-ingestor-spike"
  val region = "eu-west-2"
  val initialStreamPosition = InitialPositionInStream.LATEST
  val credentialsProvider = InstanceProfileCredentialsProvider.getInstance()
  val loadEventStream = "dpr-kinesis-partitioned-load-events-spike"
  val cdcEventStream = "dpr-kinesis-partitioned-cdc-events-spike"

  val kinesisClient = AmazonKinesisClientBuilder.standard()
    .withCredentials(credentialsProvider)
    .withRegion(region)
    .build();

  def main(sysArgs: Array[String]) {

    val kinesisClientLibConfiguration: KinesisClientLibConfiguration = {
      // TODO - review this since this is deprecated - is there a builder?
      new KinesisClientLibConfiguration(
        appName,
        streamName,
        credentialsProvider,
        workerId
      )
        .withInitialPositionInStream(initialStreamPosition)
        .withRegionName(region)
    }

    println("Job started")

    val recordProcessorFactory = RecordProcessor

    val worker = new Worker.Builder()
      .recordProcessorFactory(recordProcessorFactory)
      .config(kinesisClientLibConfiguration)
      .build()

    worker.run()

    println(s"Job completed")

    Job.commit()
  }
}

class RecordProcessor extends IRecordProcessor {

  // mutable field an artifact of the java API :/
  private var kinesisShardId: String = null

  // Checkpoint about once a minute - mutable state as per existing Java examples
  private var checkPointIntervalMs = 60000L
  private var nextCheckpointTimeMs = 0L


  override def initialize(s: String): Unit = {
    println(s"RecordProcessor initialized for shard: $s ")
    kinesisShardId = s
  }

  override def processRecords(list: util.List[Record], iRecordProcessorCheckpointer: IRecordProcessorCheckpointer): Unit = {
    println(s"Shard: $kinesisShardId Processing ${list.size()} records")

    val startTimeMs = System.currentTimeMillis()

    // Use jackson with scala support since it's already on the classpath.
    val reader = new ObjectMapper()
      .registerModule(DefaultScalaModule)
      .readerFor(classOf[Map[String, String]])

    list
      .asScala
      .foreach { record =>

        val data: String = StandardCharsets.UTF_8.decode(record.getData.duplicate()).toString

        // TODO - top level entries that aren't objects may cause issues, but only on accessing them
        val parsed: Map[String, Map[String, String]] = reader.readValue(data)

        val metadata = parsed.get("metadata")

        // TODO - switch to batch publishing - writing individual records is suboptimal
        metadata.foreach { m =>
          m.get("operation") match {
            case Some("load") => publishRecord(GlueApp.loadEventStream, record)
            case _ => publishRecord(GlueApp.cdcEventStream, record)
          }
        }

      }

    val runTime = System.currentTimeMillis() - startTimeMs

    println(s"Shard: $kinesisShardId Processed ${list.size()} records in $runTime ms - approx ${runTime / list.size()} ms per record")

    handleCheckpoint(iRecordProcessorCheckpointer)
  }

  // TODO - handle publish failures and rework to batch to minimise number of kinesis client calls
  def publishRecord(streamName: String, record: Record): Unit = {
    val putRecordRequest = new PutRecordRequest
    putRecordRequest.setData(record.getData)
    putRecordRequest.setStreamName(streamName)
    // Re-use the existing partitionKey
    putRecordRequest.setPartitionKey(record.getPartitionKey)

    GlueApp.kinesisClient.putRecord(putRecordRequest)
  }

  private def handleCheckpoint(checkPointer: IRecordProcessorCheckpointer): Unit = {
    if (System.currentTimeMillis > nextCheckpointTimeMs) {
      // TODO - this should handle backoff/retry and shutdown cases
      //      - see https://github.com/aws/aws-sdk-java/blob/master/src/samples/AmazonKinesis/AmazonKinesisApplicationSampleRecordProcessor.java
      println(s"Calling checkpoint")
      checkPointer.checkpoint()
      println(s"Checkpoint complete")
      nextCheckpointTimeMs = System.currentTimeMillis + checkPointIntervalMs
    }
  }

  override def shutdown(iRecordProcessorCheckpointer: IRecordProcessorCheckpointer, shutdownReason: ShutdownReason): Unit = {
    println(s"Shutting down record processor for shard: $kinesisShardId")
    // Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
    // TODO - as above a number of cases aren't handled here
    if (shutdownReason eq ShutdownReason.TERMINATE) iRecordProcessorCheckpointer.checkpoint()
  }

}

object RecordProcessor extends IRecordProcessorFactory {
  def createProcessor() = new RecordProcessor()
}