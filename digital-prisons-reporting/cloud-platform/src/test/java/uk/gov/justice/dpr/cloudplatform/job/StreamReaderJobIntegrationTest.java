package uk.gov.justice.dpr.cloudplatform.job;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import uk.gov.justice.dpr.BaseSparkTest;
import uk.gov.justice.dpr.cloudplatform.configuration.CloudPlatform;

@RunWith(MockitoJUnitRunner.class)
public class StreamReaderJobIntegrationTest extends BaseSparkTest {
	
	
	@Test
	@Ignore
	public void shouldRunWithRemoteS3() {
		Map<String,String> parameters = new HashMap<String, String>();
		// Zones
		parameters.put("raw.path", "s3a://moj-cloud-platform/raw");
		parameters.put("structured.path", "s3a://moj-cloud-platform/structured"); 
		parameters.put("curated.path", "s3a://moj-cloud-platform/curated");
		
		// Source Kinesis
		parameters.put("source.url", "https://kinesis.eu-west-1.amazonaws.com");
		parameters.put("source.stream", "moj-target-stream");
		parameters.put("source.accessKey", accessKey);
		parameters.put("source.secretKey", secretKey);
		
		// Sink Kinesis
		parameters.put("sink.url", "https://kinesis.eu-west-1.amazonaws.com");
		parameters.put("sink.stream", "moj-domain-stream");
		parameters.put("sink.accessKey", accessKey);
		parameters.put("sink.secretKey", secretKey);
		
		final BaseReportingHubJob job = CloudPlatform.initialise(spark, parameters);

		@SuppressWarnings("rawtypes")
		final DataStreamWriter writer = job.run()
				.trigger(Trigger.Once())
				.option("checkpointLocation", folder.getRoot().getAbsolutePath() + "/checkpoint/");
		
		try {
			final StreamingQuery query = writer.start();
			query.awaitTermination();
		} catch(Exception e) {
			e.printStackTrace();
		} 
	}
	
	
	@Test
	@Ignore
	public void shouldRunWithLocalFiles() {
		Map<String,String> parameters = new HashMap<String, String>();
		// Zones
		parameters.put("raw.path", folder.getRoot().getAbsolutePath() + "/raw");
		parameters.put("structured.path", folder.getRoot().getAbsolutePath() + "/structured"); 
		parameters.put("curated.path", folder.getRoot().getAbsolutePath() + "/curated");
		
		// Source Kinesis
		parameters.put("source.url", "https://kinesis.eu-west-1.amazonaws.com");
		parameters.put("source.stream", "moj-target-stream");
		parameters.put("source.accessKey", accessKey);
		parameters.put("source.secretKey", secretKey);
		
		// Sink Kinesis
		parameters.put("sink.url", "https://kinesis.eu-west-1.amazonaws.com");
		parameters.put("sink.stream", "moj-domain-stream");
		parameters.put("sink.accessKey", accessKey);
		parameters.put("sink.secretKey", secretKey);
		
		final BaseReportingHubJob job = CloudPlatform.initialise(spark, parameters);

		@SuppressWarnings("rawtypes")
		final DataStreamWriter writer = job.run()
				.trigger(Trigger.Once())
				.option("checkpointLocation", folder.getRoot().getAbsolutePath() + "/checkpoint/");
		
		
		while(true) {
			try {
				final StreamingQuery query = writer.start();
				query.awaitTermination();
			} catch(Exception e) {
				e.printStackTrace();
			} 
			break;
		}
		
		
	}
}
