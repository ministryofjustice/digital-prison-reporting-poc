package uk.gov.justice.dpr.kinesis;

import java.io.IOException;

import static org.mockito.Mockito.when;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsResult;

import uk.gov.justice.dpr.BaseSparkTest;
import uk.gov.justice.dpr.cdc.EventConverter;

@RunWith(MockitoJUnitRunner.class)
public class KinesisWriterTest extends BaseSparkTest {
	

	@Mock AmazonKinesis client;
	
	@Test
	@Ignore("Writes to real stream")
	public void shouldWriteToKinesisProducer() throws Exception {
		final KinesisConfiguration config = new KinesisConfiguration();
		
		config.setAwsAccessKeyId(this.accessKey);
		config.setAwsSecretKey(this.secretKey);
		config.setRegion("eu-west-1");
		config.setStream("moj-domain-stream");
		
		final KinesisWriter writer = new KinesisWriter(config);
		
		// load a batch
		final Dataset<Row> data  = getValidDataset();
		writer.writeBatch(data, 0);
		
		System.out.println("written");
	}
	
	@Test
	public void shouldWriteBatchesToAKinesisProducer() throws Exception {
		// create a mock KinesisProducer
		final KinesisConfiguration config = new KinesisConfiguration();
		
		config.setAwsAccessKeyId(this.accessKey);
		config.setAwsSecretKey(this.secretKey);
		config.setRegion("eu-west-1");
		config.setStream("moj-domain-stream");
		
		KinesisProducer producer = new KinesisProducer();
		producer.streamName = "moj-domain-stream";
		producer.config = config;
		producer.client = client;
		KinesisProducer.CACHE.put(config, producer);
		
		// now when we call it will hit the mock client
		final PutRecordsResult result = new PutRecordsResult();
		result.setFailedRecordCount(0);
		when(client.putRecords(any(PutRecordsRequest.class))).thenReturn(result);
		
		final KinesisWriter writer = new KinesisWriter(config);
		final Dataset<Row> data  = getValidDataset();
		assertEquals(1962, data.count());
		
		writer.writeBatch(data.repartition(20), 0);
	
		verify(client, times(20)).putRecords(any(PutRecordsRequest.class));
	}

	private Dataset<Row> getValidDataset() throws IOException {
		final Dataset<Row> df = this.loadParquetDataframe("/sample/events/event-converter.parquet", "event-converter.parquet");
		return df.select("partitionKey", "data");
	}
}
