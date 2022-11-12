package uk.gov.justice.dpr.kinesis;

import java.io.IOException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Ignore;
import org.junit.Test;

import uk.gov.justice.dpr.BaseSparkTest;
import uk.gov.justice.dpr.cdc.EventConverter;

public class KinesisWriterTest extends BaseSparkTest {
	
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

	private Dataset<Row> getValidDataset() throws IOException {
		final Dataset<Row> df = this.loadParquetDataframe("/sample/events/updates.parquet", "updates.parquet");
		return EventConverter.toKinesis(df);
	}
}
