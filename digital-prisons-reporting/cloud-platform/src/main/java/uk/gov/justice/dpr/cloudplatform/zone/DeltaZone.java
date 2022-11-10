package uk.gov.justice.dpr.cloudplatform.zone;

import static org.apache.spark.sql.functions.col;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import uk.gov.justice.dpr.cdc.EventConverter;
import uk.gov.justice.dpr.cloudplatform.service.SourceReferenceService;
import uk.gov.justice.dpr.delta.DeltaLakeService;

public abstract class DeltaZone {
	
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
			try {
			
			
				System.out.println(this.getClass().getSimpleName() + "::process(" + schema + "," + table + ") started");
				
				// preprocessing is needed to ensure that we do not apply changes in the wrong order
				// for example, an insert followed by a delete of the same record results in no change
				// but a delete followed by an insert causes a record to be created.
				
				// it is best that the batch is processed into a series of sub-batches that ensure changes
				// made are applied appropriately
				
			    Dataset<Row> changes = batch.filter("(recordType == 'data' and schemaName == '" + schema +"' and tableName == '" + table + "' and (operation == 'load' or operation == 'insert' or operation == 'update' or operation == 'delete'))")
											.orderBy(col("timestamp"));
				
			    // GET PAYLOAD AND RETAIN _operation and _timestamp
			    Dataset<Row> df_payload = EventConverter.getPayload(changes);
			    
				// THIS IS THE POINT AT WHICH STRUCTURE IS APPLIED TO THE DATA
				Dataset<Row> df_applied = transform(df_payload, schema, table);
				
				
				
				final String source = SourceReferenceService.getSource(schema +"." + table);
				final String tableName = SourceReferenceService.getTable(schema +"." + table);
				final String primaryKey = SourceReferenceService.getPrimaryKey(schema +"." + table);
				
				// THIS IS THE POINT WHERE WE DROP DUPLICATES
				// DUPLICATED WILL BE THE SAME ACTION (_operation) AND SAME PRIMARY KEY
				final Dataset<Row> df_merge = removeDuplicates(df_applied, primaryKey);
				
				delta.merge(prefix, source, tableName, primaryKey, df_merge);
				
				delta.endTableUpdates(prefix, source, tableName);
				
				System.out.println(this.getClass().getSimpleName() + "::process(" + schema + "," + table + ") completed");
				
			} catch(Exception e) {
				
				System.out.println(this.getClass().getSimpleName() + "::process(" + schema + "," + table + ") failed : " + e.getMessage());
				handleError(e);
			}
		}
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
