package uk.gov.justice.dpr.cloudplatform.zone;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class StructuredZone extends DeltaZone implements Zone {
	
	// prefix = moj-cloud-platform/nomis/structured
	
	public StructuredZone(final String prefix) {
		super(prefix);
	}

	@Override // Zone
	public void writeBatch(Dataset<Row> batch, Long batchId) {
		System.out.println("StructuredZone::writeBatch(<batch>," + batchId + ")");
		process(batch);
	}
	
	@Override // DeltaZone
	protected Dataset<Row> transform(final Dataset<Row> changes, final String schema, final String table) {
		return changes;
	}
}
