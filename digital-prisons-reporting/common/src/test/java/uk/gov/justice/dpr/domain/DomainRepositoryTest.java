package uk.gov.justice.dpr.domain;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import uk.gov.justice.dpr.BaseSparkTest;

@RunWith(MockitoJUnitRunner.class)
public class DomainRepositoryTest extends BaseSparkTest {

	// shouldCreateRepository
	@Test
	public void shouldCreateRepository() {
		final DomainRepository repo = new DomainRepository(spark, null, null);
		assertNotNull(repo);
	}
	
	// shouldLoadRepositoryWith0Domains
	// shouldLoadRepositoryWithManyDomainsInAnHierarchy
	// shouldReturnNoDomainsWhenTableIsntReferences
	// shouldReturnADomainWhenTableIsReferenced
	// shouldReturnMultipleDomainsWhenTableIsReferencedInMultiplePlaces
	
	
	private void createDomainSourceFolder(final String domainSourcePath, final String... domains ) throws IOException {
		for(final String domain : domains) {
			// load the domain from resource
			final String filename = "domain-" + ThreadLocalRandom.current().nextInt(1, 9999999);
			this.createFileFromResource(domain, filename, domainSourcePath);
		}
	}
	
}
