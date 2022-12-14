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
    
    spark.setLogLevel("WARN")
    
    
	// =================================================================================
	// important delta configurations
	// these need to be in the cloud-platform
	// =================================================================================
	
	sparkSession.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    sparkSession.conf.set("spark.databricks.delta.schema.autoMerge.enabled", true)
	sparkSession.conf.set("spark.databricks.delta.optimizeWrite.enabled", true)
	sparkSession.conf.set("spark.databricks.delta.autoCompact.enabled", true)
	sparkSession.conf.set("spark.sql.legacy.charVarcharAsString", true)
	
    
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
	// sink.region     : Sink Region (eu-west-1)	
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
        "sink.region",
        
        "checkpoint.location"
        
        ).toArray)
        
    Job.init(args("JOB_NAME"), glueContext, args.asJava)

    val cp_job = uk.gov.justice.dpr.cloudplatform.configuration.CloudPlatform.initialise(sparkSession, args.asJava)

    val writer = cp_job.run() // returns DataStreamWriter - could be null
      
    Job.commit()
  }
}