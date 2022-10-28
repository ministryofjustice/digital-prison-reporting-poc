package uk.gov.justice.dpr.cloudplatform.zone;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import static org.apache.spark.sql.functions.lit;

import java.io.IOException;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import uk.gov.justice.dpr.BaseSparkTest;
import uk.gov.justice.dpr.cdc.EventConverter;

@RunWith(MockitoJUnitRunner.class)
public class StructuredZoneTest extends BaseSparkTest {
	@Test
	public void shouldCreateStructuredZone() {
		final String path = folder.getRoot().getAbsolutePath() + "/structured";
		final StructuredZone zone = new StructuredZone(path);
		assertNotNull(zone);
	}
	
	// shouldDoSimpleInsert
	@Test
	public void shouldDoSimpleInsert() throws IOException {
		final String path = folder.getRoot().getAbsolutePath() + "/structured";
		final StructuredZone zone = new StructuredZone(path);
		// load a single record 
		Dataset<Row> df = getEvent("load");
		
		zone.process(df);
		
		Dataset<Row> output = zone.delta.load(path, "SYSTEM", "OFFENDERS");
		assertEquals(df.count(), output.count());
	}
	
	
	// shouldDoSimpleUpdate
	@Test
	public void shouldDoSimpleUpdate() throws IOException {
		final String path = folder.getRoot().getAbsolutePath() + "/structured";
		final StructuredZone zone = new StructuredZone(path);
		// load a single record 
		Dataset<Row> load = getEvent("load");
		Dataset<Row> update = getEvent("update");
		
		zone.process(load);
		
		Dataset<Row> output = zone.delta.load(path, "SYSTEM", "OFFENDERS");
		assertEquals(load.count(), output.count());
		
		// now update
		zone.process(update);
		output = zone.delta.load(path, "SYSTEM", "OFFENDERS");
		assertEquals(load.count(), output.count());

	}
	
	
	// shouldDoSimpleDelete
	@Test
	public void shouldDoSimpleDelete() throws IOException {
		final String path = folder.getRoot().getAbsolutePath() + "/structured";
		final StructuredZone zone = new StructuredZone(path);
		// load a single record 
		Dataset<Row> load = getEvent("load");
		Dataset<Row> delete = getEvent("delete");
		
		zone.process(load);
		
		Dataset<Row> output = zone.delta.load(path, "SYSTEM", "OFFENDERS");
		assertEquals(load.count(), output.count());
		
		// now update
		zone.process(delete);
		output = zone.delta.load(path, "SYSTEM", "OFFENDERS");
		assertEquals(0, output.count());

	}
	
	// shouldDoUpdateWhenOneColumnIsNullAndTheSecondIsNot
	@Test
	public void shouldDoUpdateOfNull() throws IOException {
		final String path = folder.getRoot().getAbsolutePath() + "/structured";
		final StructuredZone zone = new StructuredZone(path);
		// load a single record 
		Dataset<Row> load = getEvent("load");
		Dataset<Row> update = getEvent("update");
		
		load.withColumn("MIDDLE_NAME", lit(null).cast("string"));
		
		zone.process(load);
		
		Dataset<Row> output = zone.delta.load(path, "SYSTEM", "OFFENDERS");
		assertEquals(load.count(), output.count());
		
		// now update
		zone.process(update);
		output = zone.delta.load(path, "SYSTEM", "OFFENDERS");
		assertEquals(load.count(), output.count());

	}
	
	// shouldHandleInsertUpdateDeleteInOrder
	// shouldHandleDeleteInsertUpdateInOrder
	// shouldHandleMultipleUpdatesOfDifferentFieldsInOrder
	
	
	protected Dataset<Row> inject(final Dataset<Row> df, final String column, final Column col) {
		return null;
	}
	
	protected Dataset<Row> getEvent(final String operation) throws IOException {
		final Dataset<Row> df = this.loadParquetDataframe("/sample/events/kinesis.parquet", Math.random() + "kinesis.parquet");
		final Dataset<Row> result = EventConverter.fromKinesis(df).withColumn("operation", lit(operation)).limit(1);
		return result;
	}
}
