package uk.gov.justice.dpr.cloudplatform.zone;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class CuratedZone extends DeltaZone implements Zone {

	public CuratedZone(String prefix) {
		super(prefix);
	}

	@Override // Zone
	public void writeBatch(Dataset<Row> batch, Long batchId) {
		System.out.println("CuratedZone::writeBatch(<batch>," + batchId + ")");
		process(batch);
	}
	
	@Override // DeltaZone
	protected Dataset<Row> transform(final Dataset<Row> changes, final String schema, final String table) {
		return changes;
	}
}
