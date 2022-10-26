package uk.gov.justice.dpr.domainplatform.domain;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import uk.gov.justice.dpr.BaseSparkTest;

@RunWith(MockitoJUnitRunner.class)
public class DomainRepositoryTest extends BaseSparkTest {

	// shouldCreateRepository
	@Test
	public void shouldCreateRepository() {
		final DomainRepository repo = new DomainRepository(spark, null);
		assertNotNull(repo);
	}
	
	// shouldLoadRepositoryWith0Domains
	// shouldLoadRepositoryWithManyDomainsInAnHierarchy
	// shouldReturnNoDomainsWhenTableIsntReferences
	// shouldReturnADomainWhenTableIsReferenced
	// shouldReturnMultipleDomainsWhenTableIsReferencedInMultiplePlaces
}
