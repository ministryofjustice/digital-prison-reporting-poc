package uk.gov.justice.dpr.cloudplatform.configuration;

import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import uk.gov.justice.dpr.cloudplatform.job.Job;

@RunWith(MockitoJUnitRunner.class)
public class CloudPlatformTest {

	@Mock SparkSession spark;
	@Mock DataStreamReader dsr;
	
	@Before
	public void before() {
		when(spark.readStream()).thenReturn(dsr);
		when(dsr.format(any())).thenReturn(dsr);
		when(dsr.option(any(), any())).thenReturn(dsr);
	}
	
	@Test
	public void shouldCreateAJobWithTheRightConfiguration() {
		
		Map<String,String> parameters = new HashMap<String, String>();
		parameters.put("raw.path", "raw.path");
		parameters.put("structured.path", "structured.path"); 
		parameters.put("curated.path", "curated.path");
		parameters.put("sink.region", "sink.region");
		parameters.put("sink.stream", "sink.stream");

		parameters.put("source.url", "source.url");
		parameters.put("source.stream", "source.stream");
		
		final Job job = CloudPlatform.initialise(spark, parameters);
		
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
