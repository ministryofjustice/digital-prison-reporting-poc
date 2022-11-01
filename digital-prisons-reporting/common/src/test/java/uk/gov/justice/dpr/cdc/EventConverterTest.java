package uk.gov.justice.dpr.cdc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import uk.gov.justice.dpr.BaseSparkTest;
import uk.gov.justice.dpr.cdc.EventConverter;


@RunWith(MockitoJUnitRunner.class)
public class EventConverterTest extends BaseSparkTest {
	
	@Test
	public void shouldConvertMessageFromKinesisToCommonFormat() throws IOException {
		final Dataset<Row> df = this.loadParquetDataframe("/sample/events/kinesis.parquet", "kinesis.parquet");
		final Dataset<Row> result = EventConverter.fromKinesis(df);
		// check the result
		assertEquals(df.count(), result.count());
		
		// we drop data, streamName
		
		assertTrue("partitionKey missing", hasField(result, "partitionKey"));
		assertTrue("sequenceNumber missing", hasField(result, "sequenceNumber"));
		assertTrue("approximateArrivalTimestamp missing", hasField(result, "approximateArrivalTimestamp"));
		
		assertTrue("jsonData missing", hasField(result, "jsonData"));
		assertTrue("metadata missing", hasField(result, "metadata"));
		assertTrue("payload missing", hasField(result, "payload"));
		assertTrue("timestamp missing", hasField(result, "timestamp"));
		assertTrue("recordType missing", hasField(result, "recordType"));
		assertTrue("operation missing", hasField(result, "operation"));
		assertTrue("partitionKeyType missing", hasField(result, "partitionKeyType"));
		assertTrue("schemaName missing", hasField(result, "schemaName"));
		assertTrue("tableName missing", hasField(result, "tableName"));
		assertTrue("transactionId missing", hasField(result, "transactionId"));
		
	}
}
