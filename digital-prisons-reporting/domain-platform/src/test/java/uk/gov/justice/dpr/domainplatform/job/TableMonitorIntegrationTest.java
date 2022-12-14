package uk.gov.justice.dpr.domainplatform.job;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.fasterxml.jackson.databind.ObjectMapper;

import uk.gov.justice.dpr.BaseSparkTest;
import uk.gov.justice.dpr.ResourceLoader;
import uk.gov.justice.dpr.domain.DomainRepository;
import uk.gov.justice.dpr.domain.model.DomainDefinition;
import uk.gov.justice.dpr.domainplatform.configuration.DomainPlatform;
import uk.gov.justice.dpr.domainplatform.domain.DomainExecutorTest;

@RunWith(MockitoJUnitRunner.class)
public class TableMonitorIntegrationTest extends BaseSparkTest {

	@Test
	@Ignore
	public void shouldRunWithLocalFiles() throws IOException {
		
		final String domainSourcePath = folder.getRoot().getAbsolutePath() + "/domains";
		final String domainRepoPath = folder.getRoot().getAbsolutePath() + "/repo";
		
		loadDomainEnvironment(domainSourcePath, domainRepoPath);
		
		Map<String,String> parameters = new HashMap<String, String>();
		
		parameters.put("domain.repo.path", domainRepoPath);
		parameters.put("domain.files.path", domainSourcePath);
		parameters.put("cloud.platform.path", folder.getRoot().getAbsolutePath() + "/curated"); 
		
		parameters.put("target.path", folder.getRoot().getAbsolutePath() + "/target");
		
		// Source Kinesis
		parameters.put("source.region", "eu-west-1");
		parameters.put("source.queue", "moj-domain-event-queue");
		parameters.put("source.accessKey", accessKey);
		parameters.put("source.secretKey", secretKey);
		

		parameters.put("checkpoint.location", folder.getRoot().getAbsolutePath() + "/checkpoint");
		
		final TableChangeMonitor tcm = DomainPlatform.initialise(spark, parameters);

		tcm.run();
		
//		@SuppressWarnings("rawtypes")
//		final DataStreamWriter writer = tcm.run()
//				.trigger(Trigger.Once())
//				.option("checkpointLocation", folder.getRoot().getAbsolutePath() + "/checkpoint/");
//		
//		
//		while(true) {
//			try {
//				final StreamingQuery query = writer.start();
//				query.awaitTermination();
//			} catch(Exception e) {
//				e.printStackTrace();
//			} 
//			break;
//		}
		
		
	}
	
	private void loadDomainEnvironment(final String domainSourcePath, final String domainRepoPath) throws IOException {
		// list the domains we are happy to map
		createDomainSourceFolder("domains", "/sample/domain/domain-system-offenders.json", "/domains/incident.domain.json");
		
		final DomainRepository repo = new DomainRepository(spark, domainSourcePath, domainRepoPath);
		repo.touch();

	}
	
	private void createDomainSourceFolder(final String domainSourcePath, final String... domains ) throws IOException {
		try {
			folder.newFolder("domains");
		} catch (IOException e) {
			e.printStackTrace();
		}
		for(final String domain : domains) {
			// load the domain from resource
			final String filename = "domain-" + ThreadLocalRandom.current().nextInt(1, 9999999);
			this.createFileFromResource(domain, filename, domainSourcePath);
		}
	}
	
	protected DomainDefinition getDomain(final String resource) throws IOException {
		final ObjectMapper mapper = new ObjectMapper();
		final String json = ResourceLoader.getResource(DomainExecutorTest.class, resource);
		final DomainDefinition definition = mapper.readValue(json, DomainDefinition.class);
		return definition;
	}
}
