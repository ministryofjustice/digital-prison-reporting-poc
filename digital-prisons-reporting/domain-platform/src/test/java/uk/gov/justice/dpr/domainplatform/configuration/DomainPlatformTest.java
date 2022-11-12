package uk.gov.justice.dpr.domainplatform.configuration;

import static org.junit.Assert.assertNotNull;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import uk.gov.justice.dpr.BaseSparkTest;
import uk.gov.justice.dpr.domainplatform.job.TableChangeMonitor;

@RunWith(BlockJUnit4ClassRunner.class)
public class DomainPlatformTest extends BaseSparkTest {
	
	
	
	@Test
	public void shouldCreateAJobWithTheRightConfiguration() {
		
		Map<String,String> parameters = new HashMap<String, String>();
		parameters.put("domain.files.path", "domain.files.path");
		parameters.put("domain.repo.path", "domain.repo.path");
		parameters.put("cloud.platform.path", "cloud.platform.path"); 
		parameters.put("source.queue", "source.queue");
		parameters.put("source.region", "source.region");
		parameters.put("target.path", "target.path");
		
		final TableChangeMonitor tcm = DomainPlatform.initialise(spark, parameters);
		
		assertNotNull(tcm);
	
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
