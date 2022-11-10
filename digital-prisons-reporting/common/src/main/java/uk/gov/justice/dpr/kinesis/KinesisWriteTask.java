package uk.gov.justice.dpr.kinesis;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import org.apache.spark.api.java.function.ForeachPartitionFunction;

public class KinesisWriteTask  implements AutoCloseable, Serializable {
	private static final long serialVersionUID = 8360641726995716250L;
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
				final String pkey = UUID.randomUUID().toString();
				final KinesisProducer producer = KinesisProducer.getOrCreate(config);
				final Map<String, ByteBuffer> rows = new HashMap<String,ByteBuffer>();
				while(t.hasNext()) {
					Row row = t.next();
					final String key = row.getString(row.fieldIndex("partitionKey"));
					final String data = row.getString(row.fieldIndex("data"));
					if(data != null) {
						rows.put(key == null ? pkey : key, ByteBuffer.wrap(data.getBytes()));
					}
				}
				producer.writeBuffer(rows);
			}
			
		});
	}

	@Override
	public void close() throws Exception {
		// do nothing for now;
	}
	
}
