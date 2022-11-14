package uk.gov.justice.dpr.kinesis;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class KinesisWriter {


	public KinesisConfiguration config;
	
	public KinesisWriter(final KinesisConfiguration config) {
		this.config = config;
	}
	
	public void writeBatch(final Dataset<Row> batch, long id) throws Exception {
		try (KinesisWriteTask task = new KinesisWriteTask(config)) {
			System.out.println("Writing to Kinesis...");
			task.execute(batch);
		} catch(Exception e) {
			handleError(e);
		} finally {
		}
	}
	
	protected static void handleError(final Exception e) {
		final StringWriter sw = new StringWriter();
		final PrintWriter pw = new PrintWriter(sw);
		e.printStackTrace(pw);
		System.err.print(sw.getBuffer().toString());
	}
}
