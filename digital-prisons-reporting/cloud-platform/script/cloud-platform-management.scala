//=================================================
// DPR-144 Cloud Platform Compact
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
	// path        : path to data storage (s3://dpr-reporting-hub/raw/cdc)
	//
	// ==================================================================================
	
    val args = GlueArgParser.getResolvedOptions(sysArgs, 
        Seq(
        "JOB_NAME", 
        "path",
        
        "checkpoint.location"
        
        ).toArray)
        
    Job.init(args("JOB_NAME"), glueContext, args.asJava)

    val compactor = uk.gov.justice.dpr.cloudplatform.configuration.CloudPlatform.management(sparkSession, args.asJava)

    compactor.compactAll() 
    compactor.vacuum()
      
    Job.commit()
  }
}