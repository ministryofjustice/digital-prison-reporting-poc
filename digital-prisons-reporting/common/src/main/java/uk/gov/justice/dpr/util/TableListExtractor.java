package uk.gov.justice.dpr.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class TableListExtractor {
	
	private static Map<String,TableTuple> TRANSLATIONS;
	
	
	static {
		TRANSLATIONS = new HashMap<String,TableTuple>();
		
		TRANSLATIONS.put("oms_owner.offenders", new TableTuple("nomis", "offenders", "OMS_OWNER", "OFFENDERS"));
		TRANSLATIONS.put("oms_owner.offender_bookings", new TableTuple("nomis", "offender_bookings", "OMS_OWNER", "OFFENDER_BOOKINGS"));
		TRANSLATIONS.put("oms_owner.agency_locations", new TableTuple("nomis", "agentcy_locations", "OMS_OWNER", "AGENCY_LOCATIONS"));
		TRANSLATIONS.put("system.offenders", new TableTuple("nomis", "offenders", "SYSTEM", "OFFENDERS" ));
		TRANSLATIONS.put("system.offender_bookings", new TableTuple("nomis", "offender_bookings", "SYSTEM", "OFFENDER_BOOKINGS" ));
		TRANSLATIONS.put("system.agency_locations", new TableTuple("nomis", "agentcy_locations", "SYSTEM", "AGENCY_LOCATIONS"));
		
		TRANSLATIONS.put("public.report", new TableTuple("use_of_force", "report", "public", "report"));
		TRANSLATIONS.put("public.statement", new TableTuple("use_of_force", "statement", "public", "statement"));
		
	}

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
	
	// TODO : this whole thing needs replacing by translation of SCHEMAS and tables AT SOURCE
	public static List<TableTuple> extractTranslatedTableList(final Dataset<Row> df) {
		final List<Row> t = df.select("schemaName", "tableName").distinct().collectAsList();
		final List<TableTuple> tables = new ArrayList<TableTuple>();
		for(final Row r : t) {
			final String schema = r.getAs("schemaName");
			final String table = r.getAs("tableName");
			if(TRANSLATIONS.containsKey(schema.toLowerCase() +"." + table.toLowerCase())) {
				tables.add(TRANSLATIONS.get(schema.toLowerCase() +"." + table.toLowerCase()));
			}	else {
				tables.add(new TableTuple(schema, table));
			}
		}
		return tables;
	}
	
	public static class TableTuple {
		private String schema;
		private String table;
		
		private String originalSchema;
		private String originalTable;
		
		public TableTuple() {
			
		}
		
		public TableTuple(final String source) {
			if(source != null && source.contains(".")) {
				this.schema = source.split("\\.")[0];
				this.table = source.split("\\.")[1];
			}
			this.originalSchema = schema;
			this.originalTable = table;
		}
		
		public TableTuple(final String schema, final String table) {
			this.schema = schema;
			this.table = table;

			this.originalSchema = schema;
			this.originalTable = table;
		}
		
		public TableTuple(final String schema, final String table, final String origSchema, final String origTable) {
			this.schema = schema;
			this.table = table;

			this.originalSchema = origSchema;
			this.originalTable = origTable;
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

		public String getOriginalSchema() {
			return originalSchema;
		}

		public void setOriginalSchema(String originalSchema) {
			this.originalSchema = originalSchema;
		}

		public String getOriginalTable() {
			return originalTable;
		}

		public void setOriginalTable(String originalTable) {
			this.originalTable = originalTable;
		}
		
		
	}
}
