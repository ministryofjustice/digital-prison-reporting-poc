//=================================================
// DPR-109 Domain Platform
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
	// domain.repo.path    : Path to the domain repository
	// cloud.platform.path : Path to curated zone/fabric
	// source.stream       : Name of Kinesis Source Stream (dpr-domain-data-stream)
	// source.url          : Source Endpoint url (https://kinesis.eu-west-1.amazonaws.com)
	// 
	// target.path        : path to domain storage (s3://dpr-reporting-hb/domains)
	//
	// checkpoint.location : checkpoint tmp location
	// ==================================================================================
	
    val args = GlueArgParser.getResolvedOptions(sysArgs, 
        Seq(
	        "JOB_NAME", 
	        "domain.repo.path",
	        "cloud.platform.path",
	        
	        "source.stream",
	        "source.url",
	        
	        "target.path",
	        
	        "checkpoint.location"
        ).toArray)
        
    Job.init(args("JOB_NAME"), glueContext, args.asJava)

    val tableChangeMonitor = uk.gov.justice.dpr.domainplatform.configuration.DomainPlatform.initialise(sparkSession, args.asJava)

    val writer = tableChangeMonitor.run() // returns DataStreamWriter
                 .trigger(Trigger.Once)
                 .option("checkpointLocation", args("checkpoint.location"))
                 
    val query = writer.start()             // start() returns type StreamingQuery

    try {
        query.awaitTermination()
    } 
    catch {
        case e: StreamingQueryException => println("Streaming Query Exception caught!: " + e);
    }
      
    Job.commit()
  }
}