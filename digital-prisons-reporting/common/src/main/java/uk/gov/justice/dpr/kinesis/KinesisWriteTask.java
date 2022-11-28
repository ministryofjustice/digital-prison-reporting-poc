package uk.gov.justice.dpr.kinesis;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;

import org.apache.spark.api.java.function.ForeachPartitionFunction;

public class KinesisWriteTask  implements AutoCloseable, Serializable {
	private static final long serialVersionUID = 8360641726995716250L;
	
	private static final long MAX_ROWS = 499;
	private static final long MAX_SIZE = 5242000L;
	
	public KinesisConfiguration config;
	
	public KinesisWriteTask(final KinesisConfiguration config) {
		this.config = config;
	}
	
	public void execute(final Dataset<Row> df) {
		// get the Kinesis Producer
		df.foreachPartition(new ForeachPartitionFunction<Row>() {
			private static final long serialVersionUID = 5538254166625725723L;

			@Override
			public void call(Iterator<Row> t) throws Exception {
				try {
					System.out.println("Writing partition to Kinesis...");
					final String pkey = UUID.randomUUID().toString();
					final KinesisProducer producer = KinesisProducer.getOrCreate(config);
					// maximum of 500 records or 5MB
					final List<PutRecordsRequestEntry> rows = new ArrayList<PutRecordsRequestEntry>();
					long sz = 0L;
					while(t.hasNext()) {
						Row row = t.next();
						final String key = row.getString(row.fieldIndex("partitionKey"));
						byte[] data = null;
						try {
							Object o = row.get(row.fieldIndex("data"));
							data = o == null ? null : String.valueOf(o).getBytes();
						} catch(Exception e) {
							try {
								data = row.<byte[]>getAs(row.fieldIndex("data"));
							} catch(Exception ee) {
								data = null;
							}
						}
						if(data != null) {
							sz += data.length;
							rows.add(new PutRecordsRequestEntry().withData(ByteBuffer.wrap(data)).withPartitionKey(key == null ? pkey : key));
						}
						if(rows.size() >= MAX_ROWS || sz >= MAX_SIZE) {
							producer.writeBuffer(rows);
							System.out.println("Written partition of " + rows.size() + " rows [" + sz + " bytes]");
							sz = 0L;
							rows.clear();
						}
					}
					// flush the remaining
					producer.writeBuffer(rows);
					System.out.println("Written partition of " + rows.size() + " rows [" + sz + " bytes]");
				} catch(Exception e) {
					handleError(e);
				}
			}
			
		});
	}

	@Override
	public void close() throws Exception {
		// do nothing for now;
	}
	
	protected static void handleError(final Exception e) {
		final StringWriter sw = new StringWriter();
		final PrintWriter pw = new PrintWriter(sw);
		e.printStackTrace(pw);
		System.err.print(sw.getBuffer().toString());
	}
}
