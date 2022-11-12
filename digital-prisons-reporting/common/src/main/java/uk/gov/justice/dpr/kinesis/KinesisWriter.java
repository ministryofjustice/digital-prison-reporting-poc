package uk.gov.justice.dpr.kinesis;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class KinesisWriter {


	public KinesisConfiguration config;
	
	public KinesisWriter(final KinesisConfiguration config) {
		this.config = config;
	}
	
	public void writeBatch(final Dataset<Row> batch, long id) throws Exception {
		try (KinesisWriteTask task = new KinesisWriteTask(config)) {
			task.execute(batch);
		} finally {
		}
	}
}
