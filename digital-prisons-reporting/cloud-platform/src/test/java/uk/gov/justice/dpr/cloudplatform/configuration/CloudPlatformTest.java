package uk.gov.justice.dpr.cloudplatform.configuration;

import static org.junit.Assert.assertNotNull;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import uk.gov.justice.dpr.BaseSparkTest;
import uk.gov.justice.dpr.cloudplatform.job.BaseReportingHubJob;

@RunWith(MockitoJUnitRunner.class)
public class CloudPlatformTest extends BaseSparkTest {

	
	@Test
	public void shouldCreateAStreamReaderJobWithTheRightConfiguration() {
		
		Map<String,String> parameters = new HashMap<String, String>();
		parameters.put("raw.path", "raw.path");
		parameters.put("structured.path", "structured.path"); 
		parameters.put("curated.path", "curated.path");
		parameters.put("sink.url", "sink.url");
		parameters.put("sink.stream", "sink.stream");

		parameters.put("source.url", "source.url");
		parameters.put("source.stream", "source.stream");
		
		final BaseReportingHubJob job = CloudPlatform.initialiseStreamReaderJob(spark, parameters);
		
		assertNotNull(job);
		assertNotNull(job.getRawZone());
		assertNotNull(job.getStructuredZone());
		assertNotNull(job.getCuratedZone());
		assertNotNull(job.getOutStream());
	
	}
	
	@Test
	public void shouldCreateAQueueReaderJobWithTheRightConfiguration() {
		
		Map<String,String> parameters = new HashMap<String, String>();
		parameters.put("raw.path", "raw.path");
		parameters.put("structured.path", "structured.path"); 
		parameters.put("curated.path", "curated.path");
		parameters.put("sink.url", "sink.url");
		parameters.put("sink.stream", "sink.stream");

		parameters.put("source.queue", "source.name");
		parameters.put("source.region", "source.region");
		
		final BaseReportingHubJob job = CloudPlatform.initialiseQueueReaderJob(spark, null, parameters);
		
		assertNotNull(job);
		assertNotNull(job.getRawZone());
		assertNotNull(job.getStructuredZone());
		assertNotNull(job.getCuratedZone());
		assertNotNull(job.getOutStream());
	
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void shouldThrowExceptionIfNoParametersAreProvided() {
		CloudPlatform.initialise(spark, null);
	}
	

	@Test(expected = IllegalArgumentException.class)
	public void shouldThrowExceptionIfEmptyParametersAreProvided() {
		CloudPlatform.initialise(spark, new HashMap<String, String>());
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void shouldThrowExceptionIfAllRequiredAreNotProvided() {

		Map<String,String> parameters = new HashMap<String, String>();
		parameters.put("raw.path", "raw.path");
		parameters.put("structured.path", "structured.path"); 
		parameters.put("curated.path", "curated.path");
		parameters.put("sink.region", "sink.region");
		// parameters.put("sink.stream", "sink.stream");
		
		CloudPlatform.initialise(spark, parameters);
		
	}
}
