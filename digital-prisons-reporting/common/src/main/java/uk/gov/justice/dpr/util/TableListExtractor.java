package uk.gov.justice.dpr.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class TableListExtractor {

	public static List<TableTuple> extractTableList(final Dataset<Row> df) {
		final List<Row> t = df.select("schemaName", "tableName").distinct().collectAsList();
		final List<TableTuple> tables = new ArrayList<TableTuple>();
		for(final Row r : t) {
			final String schema = r.getAs("schemaName");
			final String table = r.getAs("tableName");
			tables.add(new TableTuple(schema, table));
		}
		return tables;
	}
	
	public static class TableTuple {
		private String schema;
		private String table;
		
		public TableTuple() {
			
		}
		
		public TableTuple(final String schema, final String table) {
			this.schema = schema;
			this.table = table;
		}
		
		public String getSchema() {
			return schema;
		}
		public void setSchema(String schema) {
			this.schema = schema;
		}
		public String getTable() {
			return table;
		}
		public void setTable(String table) {
			this.table = table;
		}
		
		public String asString() {
			return schema + "." + table;
		}
		
		public String asString(final String sep) {
			return schema + sep + table;
		}
		
	}
}
