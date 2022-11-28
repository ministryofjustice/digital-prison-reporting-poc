package uk.gov.justice.dpr.cdc;

import static org.apache.spark.sql.functions.col;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import uk.gov.justice.dpr.BaseSparkTest;


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
	
	@Test
	public void shouldConvertMessageFromFirehoseToKinesisFormat() throws IOException {
		final InputStream events = BaseSparkTest.getStream("/sample/events/raw-on-disk-events.json");
		
		final Dataset<Row> df = EventConverter.fromRawDMS_3_4_6(spark, events);
		
		assertNotNull(df);
		assertEquals(490, df.count());

		assertTrue("partitionKey missing", hasField(df, "partitionKey"));
		assertTrue("sequenceNumber missing", hasField(df, "sequenceNumber"));
		assertTrue("approximateArrivalTimestamp missing", hasField(df, "approximateArrivalTimestamp"));
		assertTrue("data missing", hasField(df, "data"));
		
		final Dataset<Row> result = EventConverter.fromKinesis(df);
		// check the result
		assertTrue("partitionKey missing", hasField(result, "partitionKey"));
		assertTrue("sequenceNumber missing", hasField(result, "sequenceNumber"));
		assertTrue("approximateArrivalTimestamp missing", hasField(result, "approximateArrivalTimestamp"));
		
		assertEquals(df.count(), result.count());
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
	
	@Test
	public void shouldConvertSchemaIntoNullableSchemaThroughoutForPayload() throws IOException {
		final InputStream events = BaseSparkTest.getStream("/sample/events/raw-on-disk-events.json");
		final Dataset<Row> df = EventConverter.fromKinesis(EventConverter.fromRawDMS_3_4_6(spark, events));
		
		// only get the report ones
		df.show();
		
		Dataset<Row> df_filtered = df.filter("partitionKey == 'public.report'").orderBy(col("approximateArrivalTimestamp"));
		df_filtered.show(false);
		
		final Dataset<Row> df_payload = EventConverter.getPayload(df_filtered);
		
		df_payload.show(false);
		df_payload.printSchema();
		System.out.println(df_payload.schema().prettyJson());
		// one payload record per row
		assertEquals(df_filtered.count(), df_payload.count());
	}
	
}
