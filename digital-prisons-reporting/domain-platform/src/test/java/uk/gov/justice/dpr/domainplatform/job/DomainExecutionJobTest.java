package uk.gov.justice.dpr.domainplatform.job;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import com.fasterxml.jackson.databind.ObjectMapper;

import uk.gov.justice.dpr.BaseSparkTest;
import uk.gov.justice.dpr.delta.DeltaLakeService;
import uk.gov.justice.dpr.domain.model.DomainDefinition;
import uk.gov.justice.dpr.domain.model.TableDefinition.TransformDefinition;
import uk.gov.justice.dpr.domain.model.TableDefinition.ViolationDefinition;

@RunWith(BlockJUnit4ClassRunner.class)
public class DomainExecutionJobTest extends BaseSparkTest {

	
	@Test
	public void shouldInitializeDomainExecutionJob() {
		final String domainPath = folder.getRoot().getAbsolutePath() + "/domain.json";
		final String domainName = "domain";
		final String sourcePath = folder.getRoot().getAbsolutePath() + "/source-data";
		final String sourceTable = "schema.table";
		final String targetPath = "target.path";
		final String operation = "full";
		
		final DomainExecutionJob job = new DomainExecutionJob(spark, domainPath, domainName, sourcePath, sourceTable, targetPath, operation);
		
		assertNotNull(job);
		
	}
	
	@Test
	public void shouldDoIdentityDomainLoad() throws IOException {
		
		final DeltaLakeService service = new DeltaLakeService();
		final String targetPath = folder.getRoot().getAbsolutePath() + "/target";
		
		final DomainExecutionJob job = prepare(service, true, "/sample/domain/sample-domain-execution.json", targetPath);
	
		job.run();
		
		// check that the target has been applied

		assertTrue(service.exists(targetPath, "example", "prisoner"));
	}
	
	// shouldThrowExceptionWhenDomainDoesntExist
	@Test(expected = RuntimeException.class)
	public void shouldThrowExceptionWhenDomainDoesntExist() throws IOException {
		final DeltaLakeService service = new DeltaLakeService();
		final String targetPath = folder.getRoot().getAbsolutePath() + "/target";
		
		final DomainExecutionJob job = prepare(service, false, "/sample/domain/sample-domain-execution.json", targetPath);

		job.domainPath = folder.getRoot().getAbsolutePath() + "/bad-domain.json";
		job.run();		
	}
	
	
	
	// shouldRunWith0ChangesIfTableIsNotInDomain
	@Test
	public void shouldRunWith0ChangesIfTableIsNotInDomain() throws IOException {
		final DeltaLakeService service = new DeltaLakeService();
		final String targetPath = folder.getRoot().getAbsolutePath() + "/target";
		
		final DomainExecutionJob job = prepare(service, true, "/sample/domain/sample-domain-execution-bad-source-table.json", targetPath);
		job.run();	
		
		// there shouldn't be a target table
		assertFalse(service.exists(targetPath, "example", "prisoner"));
		
	}
	
	// ********************
	// Transform Tests
	// ********************
	
	// shouldNotExecuteTransformIfNoSqlExists
	@Test
	public void shouldNotExecuteTransformIfNoSqlExists() throws IOException {
		final DeltaLakeService service = new DeltaLakeService();
		final String targetPath = folder.getRoot().getAbsolutePath() + "/target";
		
		final DomainExecutionJob job = prepareForTableChange(service, targetPath);
		final Dataset<Row> inputs = getOffenders();	
		
		final TransformDefinition transform = new TransformDefinition();
		transform.setViewText("");
		
		final Dataset<Row> outputs = job.applyTransform(inputs, transform);
		assertEquals(inputs.count(), outputs.count());
		assertTrue(this.areEqual(inputs, outputs));
	}
	
	// shouldNotExecuteTransformIfSqlIsBad
	@Test
	public void shouldNotExecuteTransformIfSqlIsBad() throws IOException {
		final DeltaLakeService service = new DeltaLakeService();
		final String targetPath = folder.getRoot().getAbsolutePath() + "/target";
		
		final DomainExecutionJob job = prepareForTableChange(service, targetPath);
		final Dataset<Row> inputs = getOffenders();	
		
		final TransformDefinition transform = new TransformDefinition();
		transform.setViewText("this is bad sql and should fail");
		
		final Dataset<Row> outputs = job.applyTransform(inputs, transform);
		assertEquals(inputs.count(), outputs.count());
		assertTrue(this.areEqual(inputs, outputs));
	}
	
	// shouldDeriveNewColumnIfFunctionProvided
	@Test
	public void shouldDeriveNewColumnIfFunctionProvided() throws IOException {
		final DeltaLakeService service = new DeltaLakeService();
		final String targetPath = folder.getRoot().getAbsolutePath() + "/target";
		
		final DomainExecutionJob job = prepareForTableChange(service, targetPath);
		final Dataset<Row> inputs = getOffenders();	
		
		final TransformDefinition transform = new TransformDefinition();
		transform.setViewText("select table.*, months_between(current_date(), to_date(table.BIRTH_DATE)) / 12 as AGE_NOW from table");
		
		Dataset<Row> outputs = doTransform(job, inputs, transform, "table");
		
 		outputs.toDF().show();
		
		assertEquals(inputs.count(), outputs.count());
		assertFalse(this.areEqual(inputs, outputs));
	}
	
	// ********************
	// Violation Tests
	// ********************
	// shouldNotWriteViolationsIfThereAreNone
	@Test
	public void shouldNotWriteViolationsIfThereAreNone() throws IOException {
		final DeltaLakeService service = new DeltaLakeService();
		final String targetPath = folder.getRoot().getAbsolutePath() + "/target";
		
		final DomainExecutionJob job = prepareForTableChange(service, targetPath);
		final Dataset<Row> inputs = getOffenders();	
		
		final ViolationDefinition violation = new ViolationDefinition();
		violation.setCheck("AGE < 100");
		violation.setLocation("safety");
		violation.setName("age");
	
		final Dataset<Row> outputs = job.applyViolations(inputs, Collections.<ViolationDefinition>singletonList(violation));
		
		// outputs should be the same as inputs
		assertTrue(this.areEqual(inputs, outputs));
		// there should be no written violations
		assertFalse(service.exists(targetPath + "/safety", "violations", "age"));
	}
	
	// shouldWriteViolationsIfThereAreSome
	@Test
	public void shouldWriteViolationsIfThereAreSome() throws IOException {
		final DeltaLakeService service = new DeltaLakeService();
		final String targetPath = folder.getRoot().getAbsolutePath() + "/target";
		
		final DomainExecutionJob job = prepareForTableChange(service, targetPath);
		final Dataset<Row> inputs = getOffenders();	
		
		final ViolationDefinition violation = new ViolationDefinition();
		violation.setCheck("AGE >= 100");
		violation.setLocation("safety");
		violation.setName("young");
	
		final Dataset<Row> outputs = job.applyViolations(inputs, Collections.<ViolationDefinition>singletonList(violation));
		
		// outputs should be the same as inputs
		assertFalse(this.areEqual(inputs, outputs));
		assertTrue(outputs.isEmpty());
		
		// there should be no written violations
		assertTrue(service.exists(targetPath + "/safety", "violations", "young"));
	}
	
	
	// shouldSubtractViolationsIfThereAreSome
	
	protected DomainExecutionJob prepare(final DeltaLakeService service, final boolean load, final String resource, final String targetPath) throws IOException {
		// save domain to disk
		final String domainPath = folder.getRoot().getAbsolutePath() + "/domain.json";
		saveDomainFileToDisk(resource, "domain.json");
		
		
		// save table to source
		final String sourcePath = folder.getRoot().getAbsolutePath() + "/source-data";
		final String sourceTable = "source.table";
		
		if(load) {
			Dataset<Row> inputs = getValidDataset();	
			service.insert(sourcePath, "source", "table", "OFFENDER_ID", inputs);
		}
		
		final String domainName = "example";
		final String operation = "full";
		
		final DomainExecutionJob job = new DomainExecutionJob(spark, domainPath, domainName, sourcePath, sourceTable, targetPath, operation);
		
		return job;
	}
	
	protected DomainExecutionJob prepareForTableChange(final DeltaLakeService service, final String targetPath) {
		final String domainPath = folder.getRoot().getAbsolutePath() + "/domain.json";

		// save table to source
		final String sourcePath = folder.getRoot().getAbsolutePath() + "/source-data";
		final String sourceTable = "source.table";
		final String domainName = "example";
		final String operation = "full";
		
		final DomainExecutionJob job = new DomainExecutionJob(spark, domainPath, domainName, sourcePath, sourceTable, targetPath, operation);
		
		return job;
	}
	
	protected Dataset<Row> doTransform(final DomainExecutionJob job, final Dataset<Row> df, final TransformDefinition transform, final String source) {
		try {
			df.createOrReplaceTempView(source);
			return job.applyTransform(df, transform);
		} finally {
			spark.catalog().dropTempView(source);
		}
	}
	
	protected Path saveDomainFileToDisk(final String resource, final String filename) throws IOException {
		final ObjectMapper mapper = new ObjectMapper();
		final InputStream stream = System.class.getResourceAsStream(resource);
		final DomainDefinition definition = mapper.readValue(stream, DomainDefinition.class);
		definition.setLocation(folder.getRoot().getAbsolutePath() + "/target");
		final String json = mapper.writeValueAsString(definition);
		final File f = folder.newFile(filename);
		FileUtils.copyInputStreamToFile(new ByteArrayInputStream(json.getBytes()), f);
		return Paths.get(f.getAbsolutePath());
	}
	
	private Dataset<Row> getOffenders() throws IOException {
		final Dataset<Row> df = this.loadParquetDataframe("/sample/offenders.parquet", "offenders.parquet");
		return df;
	}
	
	private Dataset<Row> getValidDataset() throws IOException {
		final Dataset<Row> df = this.loadParquetDataframe("/sample/events/updates.parquet", "updates.parquet");
		return df;
	}
}
