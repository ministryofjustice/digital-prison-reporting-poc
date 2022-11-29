//=================================================
// DPR-109 Domain Platform
//=================================================

import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.util.Job
import java.util.Calendar
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.Trigger
import scala.collection.JavaConverters._
import org.apache.spark.sql.streaming.StreamingQueryException
import org.apache.spark.sql.streaming.DataStreamWriter
import com.amazonaws.services.glue.log.GlueLogger

object GlueApp {
  def main(sysArgs: Array[String]) {
  
    val conf: SparkConf = new SparkConf().set("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
    
    val spark: SparkContext = new SparkContext(conf)
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
	//
	// source.queue        : Name of Kinesis Source Queue (moj-domain-event-queue)
	// source.region       : Region of the queue (eu-west-1)
	// 
	// target.path        : path to domain storage (s3://dpr-reporting-hb/domains)
	//
	// checkpoint.location : checkpoint tmp location
	// ==================================================================================
	
    val args = GlueArgParser.getResolvedOptions(sysArgs, 
        Seq(
	        "JOB_NAME", 
	        "domain.repo.path",
	        "domain.files.path",
	        "cloud.platform.path",
	        
	        "source.queue",
	        "source.region",
	        
	        "target.path",
	        
	        "checkpoint.location"
        ).toArray)
        
        
    Job.init(args("JOB_NAME"), glueContext, args.asJava)

    val tableChangeMonitor = uk.gov.justice.dpr.domainplatform.configuration.DomainPlatform.initialise(sparkSession, args.asJava)

    tableChangeMonitor.run() 
      
    Job.commit()
  }
}