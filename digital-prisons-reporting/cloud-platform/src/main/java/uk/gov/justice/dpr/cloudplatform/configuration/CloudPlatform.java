package uk.gov.justice.dpr.cloudplatform.configuration;


import java.util.Map;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamReader;

import com.amazonaws.services.sqs.AmazonSQS;

import uk.gov.justice.dpr.cloudplatform.job.BaseReportingHubJob;
import uk.gov.justice.dpr.cloudplatform.job.ManagementJob;
import uk.gov.justice.dpr.cloudplatform.job.QueueReaderJob;
import uk.gov.justice.dpr.cloudplatform.job.StreamReaderJob;
import uk.gov.justice.dpr.cloudplatform.zone.CuratedZone;
import uk.gov.justice.dpr.cloudplatform.zone.RawZone;
import uk.gov.justice.dpr.cloudplatform.zone.StructuredZone;
import uk.gov.justice.dpr.configuration.BaseApplicationConfiguration;
import uk.gov.justice.dpr.kinesis.KinesisWriter;
import uk.gov.justice.dpr.queue.Queue;

public class CloudPlatform extends BaseApplicationConfiguration {

	/**
	 * Entrypoint
	 * @param spark - spark connection
	 * @param params - command line arguments
	 * @return - stream reader job instance
	 */
	public static BaseReportingHubJob initialise(final SparkSession spark, final Map<String,String> params ) {
//		return initialiseQueueReaderJob(spark, null, params);
		return initialiseStreamReaderJob(spark, params);
	}
	
/*	public static BaseReportingHubJob initialise(final SparkSession spark, final AmazonSQS sqs, final Map<String,String> params ) {
		return initialiseQueueReaderJob(spark, sqs, params);
		return initialiseStreamReaderJob(spark, params);
	}*/
	
	public static ManagementJob management(final SparkSession spark, final Map<String,String> params ) {
		final String path = getRequiredParameter(params, "path");
		return new ManagementJob(spark, path);
	}
	
	public static BaseReportingHubJob initialiseQueueReaderJob(final SparkSession spark, final AmazonSQS sqs, final Map<String,String> params ) {
		
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
		final KinesisWriter sink = getKinesisSink(spark, params);
		final Queue queue = getQueue(spark, sqs, params);
		
		// inject them 		
		final QueueReaderJob job = new QueueReaderJob(spark, queue, raw, structured, curated, sink);
		
		// return Job
		return job;
	}
	
	public static BaseReportingHubJob initialiseStreamReaderJob(final SparkSession spark, final Map<String,String> params ) {
		
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
		final KinesisWriter sink = getKinesisSink(spark, params);
		final DataStreamReader dsr = getKinesisDataStreamReader(spark, params);
		
		// inject them
//		final StreamReaderJob job = new StreamReaderJob(dsr, raw, structured, curated, sink);

		// return Job
		return new StreamReaderJob(dsr, raw, structured, curated, sink);
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
	
}
