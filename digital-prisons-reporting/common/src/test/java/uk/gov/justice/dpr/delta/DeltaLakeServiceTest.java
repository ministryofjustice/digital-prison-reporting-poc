package uk.gov.justice.dpr.delta;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import uk.gov.justice.dpr.BaseSparkTest;

@RunWith(MockitoJUnitRunner.class)
public class DeltaLakeServiceTest extends BaseSparkTest {

	
	@Test
	public void shouldCreateDeltaLakeService() {
		final DeltaLakeService service = new DeltaLakeService();
		
		assertNotNull(service);
	}
	
	@Test
	public void shouldAddDataToATable() throws IOException {
		
		final DeltaLakeService service = new DeltaLakeService();
		
		final String prefix = folder.getRoot().getAbsolutePath();

		Dataset<Row> inputs = getValidDataset();
		
		service.insert(prefix, "schema", "table", "OFFENDER_ID", inputs);
		assertTrue(service.exists(prefix, "schema", "table"));
		Dataset<Row> outputs = service.load(prefix, "schema", "table");
		assertEquals(inputs.count(), outputs.count());
	}
	
	@Test
	public void shouldDeleteDataFromATable() throws IOException {
		
		final DeltaLakeService service = new DeltaLakeService();
		
		final String prefix = folder.getRoot().getAbsolutePath();

		Dataset<Row> inputs = getValidDataset();
		
		service.insert(prefix, "schema", "delete", "OFFENDER_ID", inputs);
		assertTrue(service.exists(prefix, "schema", "delete"));
		Dataset<Row> outputs = service.load(prefix, "schema", "delete");
		assertEquals(inputs.count(), outputs.count());
		service.delete(prefix, "schema", "delete", "OFFENDER_ID", inputs);
		outputs = service.load(prefix, "schema", "delete");
		assertEquals(0, outputs.count());
	}
	
	@Test 
	public void shouldUpdateDataInATable() throws IOException {
		final DeltaLakeService service = new DeltaLakeService();
		
		final String prefix = folder.getRoot().getAbsolutePath();

		Dataset<Row> inputs = getValidDataset();
		
		service.insert(prefix, "schema", "update", "OFFENDER_ID", inputs);
		assertTrue(service.exists(prefix, "schema", "update"));
		Dataset<Row> outputs = service.load(prefix, "schema", "update");
		assertEquals(inputs.count(), outputs.count());
		Dataset<Row> one = inputs.limit(1);
		service.merge(prefix, "schema", "update", "OFFENDER_ID", one);
		outputs = service.load(prefix, "schema", "update");
		assertEquals(inputs.count(), outputs.count());
		
	}
	
	@Test
	public void shouldReplaceDataInATable() throws IOException {
		final DeltaLakeService service = new DeltaLakeService();
		
		final String prefix = folder.getRoot().getAbsolutePath();

		Dataset<Row> inputs = getValidDataset();
		
		service.insert(prefix, "schema", "replace", "OFFENDER_ID", inputs);
		Dataset<Row> outputs = service.load(prefix, "schema", "replace");
		// coalesce so that the data is realized.
		
		outputs.write().parquet(prefix + "/temp.parquet");
		
		service.delete(prefix, "schema", "replace", "OFFENDER_ID", inputs);

		// now replace the table
		outputs = spark.read().parquet(prefix + "/temp.parquet");
		service.replace(prefix, "schema", "replace", outputs);
		final Dataset<Row> replacements = service.load(prefix, "schema", "replace");
		assertEquals(inputs.count(), outputs.count());
		assertEquals(outputs.count(), replacements.count());
		
	}
	
	private Dataset<Row> getValidDataset() throws IOException {
		final Dataset<Row> df = this.loadParquetDataframe("/sample/events/updates.parquet", "updates.parquet");
		return df;
	}
}
