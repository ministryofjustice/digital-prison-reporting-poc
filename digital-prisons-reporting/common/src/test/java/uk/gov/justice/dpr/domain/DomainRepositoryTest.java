package uk.gov.justice.dpr.domain;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import uk.gov.justice.dpr.BaseSparkTest;
import uk.gov.justice.dpr.domain.model.DomainDefinition;

@RunWith(MockitoJUnitRunner.class)
public class DomainRepositoryTest extends BaseSparkTest {
	
	// shouldCreateRepository
	@Test
	public void shouldCreateRepository() {
		final DomainRepository repo = new DomainRepository(spark, null, null);
		assertNotNull(repo);
	}
	
	// shouldLoadRepositoryWith0Domains
	@Test
	public void shouldLoadRepositoryWith0Domains() {
		final String domainSourcePath = folder.getRoot().getAbsolutePath() + "/domains";
		final String domainRepoPath = folder.getRoot().getAbsolutePath() + "/repo";
		
		// add no files
		prepare();
		
		final DomainRepository repo = new DomainRepository(spark, domainSourcePath, domainRepoPath);
		repo.load();
		
		// check the repos size is 0
		assertEquals(0, getRepoSize(repo));
	}
	
	// shouldLoadRepositoryWithManyDomainsInAnHierarchy
	@Test
	public void shouldLoadRepositoryWithManyDomainsInAnHierarchy() throws IOException {
		final String domainSourcePath = folder.getRoot().getAbsolutePath() + "/domains";
		final String domainRepoPath = folder.getRoot().getAbsolutePath() + "/repo";
		
		// add files
		prepare();
		createDomainSourceFolder("domains", "/domain/domain-2-tables.json", "/domain/domain-3-tables.json");
		
		final DomainRepository repo = new DomainRepository(spark, domainSourcePath, domainRepoPath);
		repo.load();
		
		// check the repos size is 0
		assertEquals(2, getRepoSize(repo));
	}
	
	// @TODO: shouldLoadS3Repository
	
	// shouldReturnNoDomainsWhenTableIsntReferences
	@Test
	public void shouldReturnNoDomainsWhenTableIsntReferences() throws IOException {
		final String domainSourcePath = folder.getRoot().getAbsolutePath() + "/domains";
		final String domainRepoPath = folder.getRoot().getAbsolutePath() + "/repo";
		
		// add files
		prepare();
		createDomainSourceFolder("domains", "/domain/domain-2-tables.json", "/domain/domain-3-tables.json");
		
		final DomainRepository repo = new DomainRepository(spark, domainSourcePath, domainRepoPath);
		repo.load();
		
		// check the repos size is 0
		assertEquals(2, getRepoSize(repo));
		
		// get for a source table
		Set<DomainDefinition> domains = repo.getDomainsForSource("bad-source-table-that-doesnt-exist");
		
		assertEquals(0, domains.size());
	}
	
	// shouldReturnADomainWhenTableIsReferenced
	// shouldReturnMultipleDomainsWhenTableIsReferencedInMultiplePlaces
	@Test
	public void shouldReturnDomainsWhenTableIsReferenced() throws IOException {
		final String domainSourcePath = folder.getRoot().getAbsolutePath() + "/domains";
		final String domainRepoPath = folder.getRoot().getAbsolutePath() + "/repo";
		
		// add files
		prepare();
		createDomainSourceFolder("domains", "/domain/domain-2-tables.json", "/domain/domain-3-tables.json");
		
		final DomainRepository repo = new DomainRepository(spark, domainSourcePath, domainRepoPath);
		repo.load();
		
		// check the repos size is 0
		assertEquals(2, getRepoSize(repo));
		
		// get for a source table
		Set<DomainDefinition> domains = repo.getDomainsForSource("source.table");
		assertEquals(1, domains.size());
		
		// get for prisons.location
		domains = repo.getDomainsForSource("prisons.location");
		assertEquals(2, domains.size());
		
	}
	
	
	private long getRepoSize(final DomainRepository repo) {
		final Dataset<Row> df = repo.service.load(repo.domainRepositoryPath, DomainRepository.SCHEMA, DomainRepository.TABLE);
		return df.count();
	}
	
	private void prepare() {
		try {
			folder.newFolder("domains");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void createDomainSourceFolder(final String domainSourcePath, final String... domains ) throws IOException {
		for(final String domain : domains) {
			// load the domain from resource
			final String filename = "domain-" + ThreadLocalRandom.current().nextInt(1, 9999999);
			this.createFileFromResource(domain, filename, domainSourcePath);
		}
	}
	
}
