package uk.gov.justice.dpr.cloudplatform.zone;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface Zone {

	void writeBatch(final Dataset<Row> batch, Long batchId);
}
