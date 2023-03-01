package uk.gov.justice.dpr.cloudplatform.zone;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import uk.gov.justice.dpr.cdc.EventConverter;
import uk.gov.justice.dpr.delta.DeltaLakeService;
import uk.gov.justice.dpr.hudi.HudiLakeService;
import uk.gov.justice.dpr.service.SourceReferenceService;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

import static org.apache.spark.sql.functions.col;

public abstract class HudiZone  {

	protected String prefix;
	protected final HudiLakeService hudi = new HudiLakeService();

	public HudiZone(final String prefix) {
		this.prefix = prefix;
	}
	
	protected Dataset<Row> process(Dataset<Row> batch) {
		//TODO: implementation here
		return batch;
	}

	protected Dataset<Row> removeDuplicates(final Dataset<Row> changes, final String primaryKey) {
		return changes.dropDuplicates(primaryKey, "_operation");
	}

	protected Dataset<Row> transform(final Dataset<Row> changes, final String schema, final String table) {
		return changes;
	}

	protected static void handleError(final Exception e) {
		final StringWriter sw = new StringWriter();
		final PrintWriter pw = new PrintWriter(sw);
		e.printStackTrace(pw);
		System.err.print(sw.getBuffer().toString());
	}
	

}
