package uk.gov.justice.dpr.cloudplatform.zone;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;

import uk.gov.justice.dpr.cloudplatform.service.SourceReferenceService;

public class StructuredZone extends DeltaZone implements Zone {
	
	// prefix = moj-cloud-platform/nomis/structured
	
	public StructuredZone(final String prefix) {
		super(prefix);
	}

	@Override // Zone
	public Dataset<Row> writeBatch(Dataset<Row> batch, Long batchId) {
		System.out.println("StructuredZone::writeBatch(<batch>," + batchId + ")");
		return process(batch);
	}
	
	@Override // DeltaZone
	protected Dataset<Row> transform(final Dataset<Row> changes, final String schema, final String table) {
		Map<String,String> casts = SourceReferenceService.getCasts(schema + "." + table);
		if(casts != null && !casts.isEmpty()) {
			List<Column> cols = new ArrayList<Column>();
			for(final String name : changes.columns()) {
				if(casts.containsKey(name)) {
					System.out.println("Casting " + name + " as " + casts.get(name));
					cols.add(col(name).cast(casts.get(name)));
				} else {
					cols.add(col(name));
				}
			}
			Dataset<Row> df_cast = changes.select(cols.toArray(new Column[] {}));
			return df_cast;
		}
		return changes;
	}
}
