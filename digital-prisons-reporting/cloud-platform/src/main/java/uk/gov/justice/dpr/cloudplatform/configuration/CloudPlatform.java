package uk.gov.justice.dpr.cloudplatform.configuration;


import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.kinesis.KinesisSink;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.OutputMode;

import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConverters;
import uk.gov.justice.dpr.cloudplatform.job.Job;
import uk.gov.justice.dpr.cloudplatform.zone.CuratedZone;
import uk.gov.justice.dpr.cloudplatform.zone.RawZone;
import uk.gov.justice.dpr.cloudplatform.zone.StructuredZone;

public class CloudPlatform {

	
	public static Job initialise(final SparkSession spark, final Map<String,String> params ) {
		
		if(params == null || params.isEmpty()) {
			throw new IllegalArgumentException("No Parameters provided");
		}
		
		if(spark == null) {
			throw new IllegalArgumentException("Spark Session is null");
		}
		// consume configuration entries and create the appropriate objects
		final RawZone raw = getRawZone(params);
		final StructuredZone structured = getStructuredZone(params);
		final CuratedZone curated = getCuratedZone(params);
		final KinesisSink sink = getKinesisSink(spark, params);
		final DataStreamReader dsr = getKinesisDataStreamReader(spark, params);
		
		// inject them 		
		final Job job = new Job(dsr, raw, structured, curated, sink);
		
		// return Job
		return job;
	}
	
	protected static RawZone getRawZone(final Map<String,String> params) {
		// rawPath
		final String rawPath = getRequiredParameter(params, "raw.path");
		return new RawZone(rawPath);
	}
	
	protected static StructuredZone getStructuredZone(final Map<String,String> params) {
		// structured.path
		final String structuredPath = getRequiredParameter(params, "structured.path");
		return new StructuredZone(structuredPath);
	}
	
	protected static CuratedZone getCuratedZone(final Map<String,String> params) {
		// curated.path
		final String curatedPath = getRequiredParameter(params, "curated.path");
		return new CuratedZone(curatedPath);
	}
	
	
	// https://stackoverflow.com/questions/72882055/spark-structured-streaming-with-kinesis-on-localstack-error-while-fetching-shar
	// https://github.com/qubole/kinesis-sql
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
	
	protected static KinesisSink getKinesisSink(final SparkSession spark, final Map<String,String> params) {
		final String sinkUrl = getRequiredParameter(params, "sink.url");
		final String sinkStream = getRequiredParameter(params, "sink.stream");
		final String awsAccessKey = getOptionalParameter(params, "sink.accessKey");
		final String awsSecretKey = getOptionalParameter(params, "sink.secretKey");
		
		// create params
		Map<String, String> kinesisParams = new HashMap<String,String>();
		
		kinesisParams.put("streamname", sinkStream);
		kinesisParams.put("endpointurl", sinkUrl);
		if(awsAccessKey != null && !awsAccessKey.isEmpty())
			kinesisParams.put("awsaccesskeyid", awsAccessKey);
		if(awsSecretKey != null && !awsSecretKey.isEmpty())
			kinesisParams.put("awssecretkey", awsSecretKey);
		
//		kinesisParams.put("startingposition", "TRIM_HORIZON");
//		kinesisParams.put("kinesis.client.avoidEmptyBatches", "true");
		
		scala.collection.immutable.Map<String, String> kp = JavaConverters.mapAsScalaMapConverter(kinesisParams).
				asScala()
				.toMap(Predef.<Tuple2<String, String>>conforms());
		
		return new KinesisSink(spark.sqlContext(), kp, OutputMode.Update());
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
