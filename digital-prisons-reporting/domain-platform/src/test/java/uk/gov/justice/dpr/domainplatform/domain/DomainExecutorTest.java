package uk.gov.justice.dpr.domainplatform.domain;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collections;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import com.fasterxml.jackson.databind.ObjectMapper;

import uk.gov.justice.dpr.BaseSparkTest;
import uk.gov.justice.dpr.ResourceLoader;
import uk.gov.justice.dpr.cdc.EventConverter;
import uk.gov.justice.dpr.delta.DeltaLakeService;
import uk.gov.justice.dpr.domain.model.DomainDefinition;
import uk.gov.justice.dpr.domain.model.TableDefinition.TransformDefinition;
import uk.gov.justice.dpr.domain.model.TableDefinition.ViolationDefinition;
import uk.gov.justice.dpr.util.TableListExtractor.TableTuple;

@RunWith(BlockJUnit4ClassRunner.class)
public class DomainExecutorTest extends BaseSparkTest {

	

	@Test
	public void shouldInitializeDomainExecutionJob() throws IOException {
		final String sourcePath = folder.getRoot().getAbsolutePath() + "/source-data";
		final String targetPath = "target.path";
		
		final DomainDefinition domain = getDomain("/sample/domain/sample-domain-execution.json");
		final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domain);
		
		assertNotNull(executor);
		
	}
	
	// shouldRunWithFullUpdateIfTableIsInDomain
	@Test
	public void shouldRunWithFullUpdateIfTableIsInDomain() throws IOException {
		final String sourcePath = folder.getRoot().getAbsolutePath() + "/source";
		final String targetPath = folder.getRoot().getAbsolutePath() + "/target";
		
		DeltaLakeService service = new DeltaLakeService();
		
		final DomainDefinition domain = getDomain("/sample/domain/sample-domain-execution.json");
		
		final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domain);
		
		// save a source
		TableTuple table = new TableTuple("source","table");
		final Dataset<Row> df_offenders = getOffenders();
		saveDataToDisk(TableInfo.create(sourcePath, "source", "table"), df_offenders);
		
		// do Full Materialize of source to target
		executor.doFull(table);
		
		// there should be a target table
		assertTrue(service.exists(targetPath, "example", "prisoner"));
		// it should have all the offenders in it
		
		final Dataset<Row> df_refreshed = service.load(targetPath, "example", "prisoner");
		assertTrue(areEqual(df_offenders, df_refreshed));	
	}
	
	// shouldRunWithIncrementalUpdateIfTableIsInDomain
	@Test
	public void shouldRunWithIncrementalUpdateIfTableIsInDomain() throws IOException {
		final String sourcePath = folder.getRoot().getAbsolutePath() + "/source";
		final String targetPath = folder.getRoot().getAbsolutePath() + "/target";
		
		DeltaLakeService service = new DeltaLakeService();
		
		final DomainDefinition domain = getDomain("/sample/domain/sample-domain-execution.json");
		
		final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domain);
		
		// save a source
		TableTuple table = new TableTuple("source","table");
		Dataset<Row> df_offenders = getOffenders();
		// give each a new id 
		df_offenders = df_offenders.withColumn("OFFENDER_ID", functions.monotonically_increasing_id());
		saveDataToDisk(TableInfo.create(sourcePath, "source", "table"), df_offenders);
		
		// do Full Materialize of source to target
		// so as to populate the table with original data
		executor.doFull(table);
		
		// get some events
		final Dataset<Row> df_incremental = getValidDataset();
		final Dataset<Row> df_data = EventConverter.getPayload(df_incremental);
		executor.doIncremental(df_data, table);
		
		// there should be a target table
		assertTrue(service.exists(targetPath, "example", "prisoner"));
		
		// it should have all the offenders in it		
		final Dataset<Row> df_refreshed = service.load(targetPath, "example", "prisoner");
		// but they should be updated
		assertEqual(df_offenders.count(), df_refreshed.count());
		// assertTrue(areEqual(df_offenders, df_refreshed));	
	}
	
	
	private void assertEqual(long count, long count2) {
		// TODO Auto-generated method stub
		
	}

	// shouldRunWith0ChangesIfTableIsNotInDomain
	@Test
	public void shouldRunWith0ChangesIfTableIsNotInDomain() throws IOException {
		final String sourcePath = folder.getRoot().getAbsolutePath() + "/source";
		final String targetPath = folder.getRoot().getAbsolutePath() + "/target";
		final DomainDefinition domain = getDomain("/sample/domain/sample-domain-execution-bad-source-table.json");
		
		DeltaLakeService service = new DeltaLakeService();
		
		final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domain);
		TableTuple table = new TableTuple("source","table");
		final Dataset<Row> df_offenders = getOffenders();
		saveDataToDisk(TableInfo.create(sourcePath, "source", "table"), df_offenders);
		
		executor.doFull(table);
		
		// there shouldn't be a target table
		assertFalse(service.exists(targetPath, "example", "prisoner"));

	}
	
	// ********************
	// Transform Tests
	// ********************
	
	// shouldNotExecuteTransformIfNoSqlExists
	@Test
	public void shouldNotExecuteTransformIfNoSqlExists() throws IOException {
		final String sourcePath = folder.getRoot().getAbsolutePath() + "/source";
		final String targetPath = folder.getRoot().getAbsolutePath() + "/target";
		final DomainDefinition domain = getDomain("/sample/domain/sample-domain-execution.json");
				
		final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domain);
		final Dataset<Row> inputs = getOffenders();	
		
		final TransformDefinition transform = new TransformDefinition();
		transform.setViewText("");
		
		final Dataset<Row> outputs = executor.applyTransform(inputs, transform);
		assertEquals(inputs.count(), outputs.count());
		assertTrue(this.areEqual(inputs, outputs));
	}
	
	// shouldNotExecuteTransformIfSqlIsBad
	@Test
	public void shouldNotExecuteTransformIfSqlIsBad() throws IOException {
		final String sourcePath = folder.getRoot().getAbsolutePath() + "/source";
		final String targetPath = folder.getRoot().getAbsolutePath() + "/target";
		final DomainDefinition domain = getDomain("/sample/domain/sample-domain-execution.json");
				
		final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domain);
		final Dataset<Row> inputs = getOffenders();	
		
		final TransformDefinition transform = new TransformDefinition();
		transform.setViewText("this is bad sql and should fail");
		
		final Dataset<Row> outputs = executor.applyTransform(inputs, transform);
		assertEquals(inputs.count(), outputs.count());
		assertTrue(this.areEqual(inputs, outputs));
	}
	
	// shouldDeriveNewColumnIfFunctionProvided
	@Test
	public void shouldDeriveNewColumnIfFunctionProvided() throws IOException {
		final String sourcePath = folder.getRoot().getAbsolutePath() + "/source";
		final String targetPath = folder.getRoot().getAbsolutePath() + "/target";
		final DomainDefinition domain = getDomain("/sample/domain/sample-domain-execution.json");
				
		final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domain);
		final Dataset<Row> inputs = getOffenders();	
		
		final TransformDefinition transform = new TransformDefinition();
		transform.setViewText("select table.*, months_between(current_date(), to_date(table.BIRTH_DATE)) / 12 as AGE_NOW from table");
		
		Dataset<Row> outputs = doTransform(executor, inputs, transform, "table");
		
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
		final String sourcePath = folder.getRoot().getAbsolutePath() + "/source";
		final String targetPath = folder.getRoot().getAbsolutePath() + "/target";
		final DomainDefinition domain = getDomain("/sample/domain/sample-domain-execution.json");
				
		final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domain);
		final Dataset<Row> inputs = getOffenders();	
		
		final ViolationDefinition violation = new ViolationDefinition();
		violation.setCheck("AGE < 100");
		violation.setLocation("safety");
		violation.setName("age");
	
		final Dataset<Row> outputs = executor.applyViolations(inputs, Collections.<ViolationDefinition>singletonList(violation));
		
		// outputs should be the same as inputs
		assertTrue(this.areEqual(inputs, outputs));
		// there should be no written violations
		assertFalse(service.exists(targetPath + "/safety", "violations", "age"));
	}
	
	// shouldWriteViolationsIfThereAreSome
	// shouldSubtractViolationsIfThereAreSome
	@Test
	public void shouldWriteViolationsIfThereAreSome() throws IOException {
		final DeltaLakeService service = new DeltaLakeService();
		final String sourcePath = folder.getRoot().getAbsolutePath() + "/source";
		final String targetPath = folder.getRoot().getAbsolutePath() + "/target";
		final DomainDefinition domain = getDomain("/sample/domain/sample-domain-execution.json");
				
		final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domain);
		final Dataset<Row> inputs = getOffenders();	
		
		final ViolationDefinition violation = new ViolationDefinition();
		violation.setCheck("AGE >= 100");
		violation.setLocation("violations");
		violation.setName("young");
	
		final Dataset<Row> outputs = executor.applyViolations(inputs, Collections.<ViolationDefinition>singletonList(violation));

		// shouldSubtractViolationsIfThereAreSome
		// outputs should be removed
		assertFalse(this.areEqual(inputs, outputs));
		assertTrue(outputs.isEmpty());
		
		// there should be some written violations
		assertTrue(service.exists(targetPath, "violations", "young"));
	}

	
	protected Dataset<Row> doTransform(final DomainExecutor executor, final Dataset<Row> df, final TransformDefinition transform, final String source) {
		try {
			df.createOrReplaceTempView(source);
			return executor.applyTransform(df, transform);
		} finally {
			spark.catalog().dropTempView(source);
		}
	}
	
	protected DomainDefinition getDomain(final String resource) throws IOException {
		final ObjectMapper mapper = new ObjectMapper();
		final String json = ResourceLoader.getResource(DomainExecutorTest.class, resource);
		final DomainDefinition definition = mapper.readValue(json, DomainDefinition.class);
		return definition;
	}
	
	private void saveDataToDisk(final TableInfo location, final Dataset<Row> df) {
		DeltaLakeService service = new DeltaLakeService();
		service.replace(location.getPrefix(), location.getSchema(), location.getTable(), df);
	}
	
	private Dataset<Row> getOffenders() throws IOException {
		final Dataset<Row> df = this.loadParquetDataframe("/sample/offenders.parquet", "offenders.parquet");
		return df;
	}
	
	@SuppressWarnings("unused")
	private Dataset<Row> getValidDataset() throws IOException {
		final Dataset<Row> df = this.loadParquetDataframe("/sample/events/updates.parquet", "updates.parquet");
		return df;
	}
}
