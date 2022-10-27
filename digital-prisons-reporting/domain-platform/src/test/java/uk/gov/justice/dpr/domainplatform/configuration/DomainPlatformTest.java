package uk.gov.justice.dpr.domainplatform.configuration;

import static org.junit.Assert.assertNotNull;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import uk.gov.justice.dpr.BaseSparkTest;
import uk.gov.justice.dpr.domainplatform.job.DomainExecutionJob;

@RunWith(BlockJUnit4ClassRunner.class)
public class DomainPlatformTest extends BaseSparkTest {
	
	
	
	@Test
	public void shouldCreateAJobWithTheRightConfiguration() {
		
		Map<String,String> parameters = new HashMap<String, String>();
		parameters.put("domain.path", "domain.path");
		parameters.put("domain.name", "domain.name"); 
		parameters.put("domain.operation", "incremental");
		parameters.put("source.path", "source.path");
		parameters.put("source.table", "source.table");
		parameters.put("target.path", "target.path");
		
		final DomainExecutionJob job = DomainPlatform.initialise(spark, parameters);
		
		assertNotNull(job);
	
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void shouldThrowExceptionIfNoParametersAreProvided() {
		DomainPlatform.initialise(spark, null);
	}
	

	@Test(expected = IllegalArgumentException.class)
	public void shouldThrowExceptionIfEmptyParametersAreProvided() {
		DomainPlatform.initialise(spark, new HashMap<String, String>());
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void shouldThrowExceptionIfAllRequiredAreNotProvided() {

		Map<String,String> parameters = new HashMap<String, String>();
		parameters.put("raw.path", "raw.path");
		parameters.put("structured.path", "structured.path"); 
		parameters.put("curated.path", "curated.path");
		parameters.put("sink.region", "sink.region");
		// parameters.put("sink.stream", "sink.stream");
		
		DomainPlatform.initialise(spark, parameters);
		
	}
}
