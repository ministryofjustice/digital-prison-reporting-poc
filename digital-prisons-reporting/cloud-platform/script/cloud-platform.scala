//=================================================
// DPR-100 Cloud Platform
//=================================================

import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.util.Job
import java.util.Calendar
import org.apache.spark.SparkContext
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.Trigger
import scala.collection.JavaConverters._
import org.apache.spark.sql.streaming.StreamingQueryException
import com.amazonaws.services.glue.log.GlueLogger

object GlueApp {
  def main(sysArgs: Array[String]) {
  
    val spark: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(spark)
    val logger = new GlueLogger
    val sparkSession: SparkSession = glueContext.getSparkSession
    
    import sparkSession.implicits._
    
    spark.setLogLevel("INFO")
    
    
	// =================================================================================
	// important delta configurations
	// these need to be in the cloud-platform
	// =================================================================================
	
	sparkSession.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    sparkSession.conf.set("spark.databricks.delta.schema.autoMerge.enabled", true)
    
    // =================================================================================
	// Parameters
	// JOB_NAME : set by default in parameters
	//
	// source.queue    : Name of Kinesis Source Queue (moj-cdc-event-queue)
	// source.region   : Region of the queue (eu-west-1)
	// 
	// raw.path        : path to raw data storage (s3://dpr-reporting-hub/raw/cdc)
	// structured.path : path to structured storage (s3://dpr-reporting-hub/structured)
	// curated.path    : path to curated storage (s3://mdpr-reporting-hub/curated)
	//
	// sink.stream     : Name of Kinesis Sink Stream (domain-data-stream-events)
	// sink.url        : https://kinesis.eu-west-1.amazonaws.com
	// ==================================================================================
	
    val args = GlueArgParser.getResolvedOptions(sysArgs, 
        Seq(
        "JOB_NAME", 
        "source.queue",
        "source.region",
        
        "raw.path", 
        "structured.path",
        "curated.path",
        
        "sink.stream", 
        "sink.url",
        
        "checkpoint.location"
        
        ).toArray)
        
    Job.init(args("JOB_NAME"), glueContext, args.asJava)
    
    val sqsClient = AmazonSQSClientBuilder.standard().withRegion(args("source.region"))

    val cp_job = uk.gov.justice.dpr.cloudplatform.configuration.CloudPlatform.initialise(sparkSession, sqsClient, args.asJava)

    val writer = cp_job.run() // returns DataStreamWriter - could be null
    
    if(writer != null) {
    	writer
        	.trigger(Trigger.Once)
            .option("checkpointLocation", args("checkpoint.location"))
                 
    	val query = writer.start()             // start() returns type StreamingQuery

	    try {
	        query.awaitTermination()
	    } 
	    catch {
	        case e: StreamingQueryException => println("Streaming Query Exception caught!: " + e);
	    }
    }
      
    Job.commit()
  }
}