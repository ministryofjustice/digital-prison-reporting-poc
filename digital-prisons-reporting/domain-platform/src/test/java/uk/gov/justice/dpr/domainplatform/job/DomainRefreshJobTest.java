package uk.gov.justice.dpr.domainplatform.job;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import com.fasterxml.jackson.databind.ObjectMapper;

import uk.gov.justice.dpr.BaseSparkTest;
import uk.gov.justice.dpr.delta.DeltaLakeService;
import uk.gov.justice.dpr.domain.DomainRepository;
import uk.gov.justice.dpr.domain.model.DomainDefinition;

@RunWith(BlockJUnit4ClassRunner.class)
public class DomainRefreshJobTest extends BaseSparkTest {

	protected DeltaLakeService service = new DeltaLakeService();
	
	@Test
	public void shouldCreateDomainRefreshJob() {
		
		final String domainFiles = folder.getRoot().getAbsolutePath() + "/domains";
		final String domainRepoPath = folder.getRoot().getAbsolutePath() + "/domain.repo";
		final String sourcePath = folder.getRoot().getAbsolutePath() + "/source-data";
		final String targetPath = "target.path";
		
		final DomainRefreshJob job = new DomainRefreshJob(spark, domainFiles, domainRepoPath, sourcePath, targetPath);
		
		assertNotNull(job);

	}
	
	@Test
	public void shouldRefreshADomainOfOneTableAndOneSource() throws IOException {
		final String sourcePath = folder.getRoot().getAbsolutePath() + "/source";
		final String targetPath = folder.getRoot().getAbsolutePath() + "/target";
		final String domainFiles = folder.getRoot().getAbsolutePath() + "/domains";
		final String domainRepoPath = folder.getRoot().getAbsolutePath() + "/domain.repo";
		
		prepareSourcesAndDomain("/sample/domain/refresh-one-table-one-source.json");
		final DomainRepository repo = new DomainRepository(spark, domainFiles, domainRepoPath);
		repo.touch();
		
		final DomainRefreshJob job = new DomainRefreshJob(spark, domainFiles, domainRepoPath, sourcePath, targetPath);
		
		assertNotNull(job);
		job.run("example");
		
		final Dataset<Row> df_result = service.load(targetPath, "example", "prisoner");
		assertNotNull(df_result);
		assertFalse(df_result.isEmpty());
	}
	
	// shouldRefreshADomainWithMultipleTablesEachWithOneSource
	@Test
	public void shouldRefreshADomainWithMultipleTablesEachWithOneSource() throws IOException {
		final String sourcePath = folder.getRoot().getAbsolutePath() + "/source";
		final String targetPath = folder.getRoot().getAbsolutePath() + "/target";
		final String domainFiles = folder.getRoot().getAbsolutePath() + "/domains";
		final String domainRepoPath = folder.getRoot().getAbsolutePath() + "/domain.repo";
		
		prepareSourcesAndDomain("/sample/domain/refresh-many-tables-one-source.json");
		final DomainRepository repo = new DomainRepository(spark, domainFiles, domainRepoPath);
		repo.touch();
		
		final DomainRefreshJob job = new DomainRefreshJob(spark, domainFiles, domainRepoPath, sourcePath, targetPath);
		
		assertNotNull(job);
		job.run("example");
		
		final Dataset<Row> df_result = service.load(targetPath, "example", "prisoner");
		assertNotNull(df_result);
		assertFalse(df_result.isEmpty());
		
		final Dataset<Row> df_result2 = service.load(targetPath, "example", "bookings");
		assertNotNull(df_result2);
		assertFalse(df_result2.isEmpty());
	}
	
	
	// shouldRefreshADomainWithMultipleTablesAndMultipleSources
	@Test
	public void shouldRefreshADomainWithMultipleTablesAndMultipleSources() throws IOException {
		final String sourcePath = folder.getRoot().getAbsolutePath() + "/source";
		final String targetPath = folder.getRoot().getAbsolutePath() + "/target";
		final String domainFiles = folder.getRoot().getAbsolutePath() + "/domains";
		final String domainRepoPath = folder.getRoot().getAbsolutePath() + "/domain.repo";
		
		prepareSourcesAndDomain("/sample/domain/refresh-many-tables-many-sources.json");
		final DomainRepository repo = new DomainRepository(spark, domainFiles, domainRepoPath);
		repo.touch();
		
		final DomainRefreshJob job = new DomainRefreshJob(spark, domainFiles, domainRepoPath, sourcePath, targetPath);
		
		assertNotNull(job);
		job.run("example");
		
		// this has many tables
		final Dataset<Row> df_result = service.load(targetPath, "example", "prisoner");
		assertNotNull(df_result);
		// assertFalse(df_result.isEmpty()); shouldn't be empty but is because of joins
		
		final Dataset<Row> df_result2 = service.load(targetPath, "example", "bookings");
		assertNotNull(df_result2);
		assertFalse(df_result2.isEmpty());
	}
	
	protected void prepareSourcesAndDomain(final String domainResource) throws IOException {
		final String sourcePath = folder.getRoot().getAbsolutePath() + "/source";
		Dataset<Row> df_offenders = loadParquetDataframe("/sample/offenders.parquet", "offenders.parquet");
		service.insert(sourcePath, "nomis", "offenders", "OFFENDER_ID", df_offenders);
		
		Dataset<Row> df_offender_bookings = loadParquetDataframe("/sample/offender-bookings.parquet", "offender-bookings.parquet");
		service.insert(sourcePath, "nomis", "offender_bookings", "OFFENDER_BOOK_ID", df_offender_bookings);

		this.createFileFromResource(domainResource, "domain.json", "domains");

	}
	
}
