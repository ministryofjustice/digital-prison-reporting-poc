package uk.gov.justice.dpr.cloudplatform.zone;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;


public class RawZone implements Zone {
	
	private final String prefix; 
	
	public RawZone(final String prefix) {
		this.prefix = prefix;
	}

	public void writeBatch(final Dataset<Row> batch, Long batchId) {
		System.out.println("RawZone::writeBatch(<batch>, " + batchId + ")");
		batch.write()
			.mode(SaveMode.Append)
			.format("parquet")
			.save(prefix + "/"+ batchId +"/");
	}
}
