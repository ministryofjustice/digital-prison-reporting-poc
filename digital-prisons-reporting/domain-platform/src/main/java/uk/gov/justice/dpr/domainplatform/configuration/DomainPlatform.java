package uk.gov.justice.dpr.domainplatform.configuration;

import java.util.Map;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamReader;

import uk.gov.justice.dpr.domainplatform.job.TableChangeMonitor;

public class DomainPlatform {

	public static TableChangeMonitor initialise(final SparkSession spark, final Map<String,String> params) {
		if(params == null || params.isEmpty()) {
			throw new IllegalArgumentException("No Parameters provided");
		}
		
		if(spark == null) {
			throw new IllegalArgumentException("Spark Session is null");
		}

		final String domainRepoPath = getRequiredParameter(params, "domain.repo.path");
		final String sourcePath = getRequiredParameter(params, "cloud.platform.path");
		final String targetPath = getRequiredParameter(params, "target.path");

		final DataStreamReader dsr = getKinesisDataStreamReader(spark, params);
		return new TableChangeMonitor(domainRepoPath, sourcePath, targetPath, dsr);
	}
	
	protected static DataStreamReader getKinesisDataStreamReader(final SparkSession spark, final Map<String,String> params) {
		final String streamName = getRequiredParameter(params, "source.stream");
		final String endpointUrl = getRequiredParameter(params, "source.url");
		final String awsAccessKey = getOptionalParameter(params, "source.accessKey");
		final String awsSecretKey = getOptionalParameter(params, "source.secretKey");
		
		final DataStreamReader dsr = spark.readStream()   // readstream() returns type DataStreamReader
			      .format("kinesis")
			      .option("streamName", streamName)
			      .option("endpointUrl", endpointUrl)
			      // .option("checkpointInterval", <same as trigger>)
			      // .option("checkpointLocation", "/tmp")
			      // shard management
			      // .option("initialPosition", "trim_horizon")
			      .option("startingposition", "TRIM_HORIZON")
			      //.option("maxFetchRate", "1.5")
			      //.option("minFetchPeriod", "15s")
			      //.option("maxFetchDuration", "20s")
			      //.option("shardFetchInterval", "10m")
			      //.option("fetchBufferSize", "1gb")
		        
			      .option("kinesis.client.avoidEmptyBatches", "true")
		        
			      // schema and data format
			      .option("inferSchema", "true")
			      .option("classification", "json");
		
		if(awsAccessKey != null && !awsAccessKey.isEmpty() && awsSecretKey != null && !awsSecretKey.isEmpty() ) {
		      dsr.option("awsAccessKeyId", awsAccessKey)
		         .option("awsSecretKey", awsSecretKey);
		}
		
		return dsr;
	}
	
	
	protected static String getRequiredParameter(final Map<String, String> params, final String name) {
		final String value = params.getOrDefault(name, null);
		if(value == null || value.isEmpty()) 
			throw new IllegalArgumentException(name + " is a required parameter and is missing");
		
		return value;
	}
	
	protected static String getOptionalParameter(final Map<String, String> params, final String name) {
		return params.getOrDefault(name, null);
	}
}
