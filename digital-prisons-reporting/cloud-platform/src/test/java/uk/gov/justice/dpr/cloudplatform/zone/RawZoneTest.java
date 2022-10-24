package uk.gov.justice.dpr.cloudplatform.zone;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import uk.gov.justice.dpr.BaseSparkTest;
import uk.gov.justice.dpr.cloudplatform.cdc.EventConverter;

@RunWith(MockitoJUnitRunner.class)
public class RawZoneTest extends BaseSparkTest {
	
	@Test
	public void shouldCreateRawZone() {
		
		final String path = folder.getRoot().getAbsolutePath() + "/cdc";
		final RawZone zone = new RawZone(path);
		assertNotNull(zone);
		
	}
	
	@Test
	public void shouldSaveBatchToRawZone() throws IOException {
		final String path = folder.getRoot().getAbsolutePath() + "/cdc";
		final RawZone zone = new RawZone(path);

		final Dataset<Row> df = this.loadParquetDataframe("/sample/events/kinesis.parquet", "kinesis.parquet");
		final Dataset<Row> result = EventConverter.fromKinesis(df);
		
		zone.writeBatch(result, 123456L);
		
		// check it is written : add the batchId
		final Dataset<Row> rawData = spark.read().parquet(path + "/123456/");
		
		assertFalse(result.isEmpty());
		assertFalse(rawData.isEmpty());
		
		assertTrue(areEqual(result, rawData));
	}
}
