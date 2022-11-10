package uk.gov.justice.dpr.cloudplatform.job;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.Ignore;
import org.junit.Test;

import uk.gov.justice.dpr.BaseSparkTest;
import uk.gov.justice.dpr.cloudplatform.configuration.CloudPlatform;

public class QueueReaderJobIntegrationTest extends BaseSparkTest {

	@Test
	@Ignore
	public void shouldRunWithRemoteS3() {
		Map<String,String> parameters = new HashMap<String, String>();
		// Zones
		parameters.put("raw.path", "s3a://moj-cloud-platform/raw");
		parameters.put("structured.path", "s3a://moj-cloud-platform/structured"); 
		parameters.put("curated.path", "s3a://moj-cloud-platform/curated");
		
		// Source SQS
		parameters.put("source.queue", "moj-cdc-event-queue");
		parameters.put("source.region", "eu-west-1");
		parameters.put("source.accessKey", accessKey);
		parameters.put("source.secretKey", secretKey);
		
		// Sink Kinesis
		parameters.put("sink.url", "https://kinesis.eu-west-1.amazonaws.com");
		parameters.put("sink.stream", "moj-domain-stream");
		parameters.put("sink.accessKey", accessKey);
		parameters.put("sink.secretKey", secretKey);
		
		final BaseReportingHubJob job = CloudPlatform.initialise(spark, parameters);

		@SuppressWarnings("rawtypes")
		final DataStreamWriter writer = job.run();
		if(writer != null) {
			try {
				
				writer
				.trigger(Trigger.Once())
				.option("checkpointLocation", folder.getRoot().getAbsolutePath() + "/checkpoint/");
		
				final StreamingQuery query = writer.start();
				query.awaitTermination();
			} catch(Exception e) {
				e.printStackTrace();
			} 
		}
	}
}
