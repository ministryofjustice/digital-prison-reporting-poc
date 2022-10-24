package uk.gov.justice.dpr.cloudplatform.zone;

import static org.apache.spark.sql.functions.col;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import uk.gov.justice.dpr.delta.DeltaLakeService;

public abstract class DeltaZone {

	protected static final Map<String,String> primaryKeyNames;
	
	static {
		primaryKeyNames = new HashMap<String,String>();
		primaryKeyNames.put("OFFENDERS", "OFFENDER_ID");
		primaryKeyNames.put("OFFENDER_BOOKINGS", "OFFENDER_BOOK_ID");
	}
	
	protected String prefix; 
	protected final DeltaLakeService delta = new DeltaLakeService();

	public DeltaZone(final String prefix) {
		this.prefix = prefix;
	}
	
	protected void process(Dataset<Row> batch) {
		// determine the tables in 	the batch
		final List<Row> tables = batch.filter("recordType='data'").select("schemaName", "tableName").distinct().collectAsList();
		
		for(final Row t : tables) {
			
			final String schema = t.getAs("schemaName");
			final String table = t.getAs("tableName");
			
			System.out.println(this.getClass().getSimpleName() + "::process(" + schema + "," + table + ")");
			
			// preprocessing is needed to ensure that we do not apply changes in the wrong order
			// for example, an insert followed by a delete of the same record results in no change
			// but a delete followed by an insert causes a record to be created.
			
			// it is best that the batch is processed into a series of sub-batches that ensure changes
			// made are applied appropriately
			
		    Dataset<Row> changes = batch.filter("(recordType == 'data' and schemaName == '" + schema +"' and tableName == '" + table + "' and (operation == 'load' or operation == 'insert' or operation == 'update' or operation == 'delete'))")
										.orderBy(col("timestamp"));
			
			// THIS IS THE POINT AT WHICH STRUCTURE IS APPLIED TO THE DATA
			changes = transform(changes, schema, table);
			
			
			final String primaryKey = primaryKeyNames.get(table);
			
			delta.merge(prefix, schema, table, primaryKey, changes);
			
			delta.endTableUpdates(prefix, schema, table);
		}
	}
	
	protected Dataset<Row> transform(final Dataset<Row> changes, final String schema, final String table) {
		return changes;
	}
	
	protected List<Dataset<Row>> preprocess(final Dataset<Row> df, final String schema, final String table) {
		final List<Dataset<Row>> batches = new ArrayList<Dataset<Row>>();
		List<Row> rows = df.collectAsList();
		// runs of one type of operation are ok to bunch together 
		for(final Row row : rows) {
			// row.getString(row.fieldIndex("operation"))
			batches.add(df.sparkSession().createDataFrame(Arrays.<Row>asList(row), df.schema()));
		}
		return batches;
	}
}
