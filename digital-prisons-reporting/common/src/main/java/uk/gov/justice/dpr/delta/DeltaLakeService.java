package uk.gov.justice.dpr.delta;

import static org.apache.spark.sql.functions.col;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import io.delta.tables.DeltaTable;

public class DeltaLakeService {

	private final static String SOURCE = "source";
	private final static String TARGET = "target";
	
	// insert events
	public void insert(final String prefix, final String schema, final String table, final String primaryKey, final Dataset<Row> df) {
		merge(prefix, schema, table, primaryKey, df);
	}
	
	// merge events
	public void merge(final String prefix, final String schema, final String table, final String primaryKey, final Dataset<Row> df) {
		// prepare dataset
		if(!df.isEmpty()) {
			final DeltaTable dt = getTable(prefix, schema, table);
			if(dt != null) {
				
				final Map<String,String> expression = getExpression(SOURCE, TARGET, df, Arrays.<String>asList("_operation", "_timestamp"));
				
				final String pk = SOURCE + "." + primaryKey;
				final String uk = TARGET + "." + primaryKey;
				dt.as(SOURCE)
				.merge(df.as(TARGET), pk + "=" + uk )
				// delete
				.whenMatched(TARGET + "._operation=='delete'").delete()
				// update
				.whenMatched(TARGET + "._operation=='update'").updateExpr(expression)
				// insert
				// should really be whenNotMatch(TARGET + "._operation=='insert' or " + TARGET + "._operation=='load'").insertExpr(expression)
				.whenNotMatched().insertExpr(expression)
				.execute();
				
				updateManifest(dt);
			} else {
				createTable(df.drop(col("_operation")).drop(col("_timestamp")), prefix, schema, table);
			}
		}
	}
	
	public void append(final String prefix, final String schema, final String table, final Dataset<Row> df) {
		df.write()
		.format("delta")
		.mode("append")
		.option("path", getTablePath(prefix, schema, table))
		.saveAsTable(table);
	}
	
	public void replace(final String prefix, final String schema, final String table, final Dataset<Row> df) {
		df.write()
			.format("delta")
			.mode("overwrite")
			.option("overwriteSchema", true)
			.option("path", getTablePath(prefix, schema, table))
			.saveAsTable(table);
	}
	
	public void vacuum(final String prefix, final String schema, final String table) {
		final DeltaTable dt = getTable(prefix, schema, table);
		if(dt != null) {
			dt.vacuum();
		}
	}
	
	public void delete(final String prefix, final String schema, final String table, final String primaryKey, final Dataset<Row> df) {
		final DeltaTable dt = getTable(prefix, schema, table);
		if( dt != null) {
			if(!df.isEmpty()) {
				final List<Object> keys = df.select(primaryKey).distinct().map(
						new ObjectRowMapper(), Encoders.javaSerialization(Object.class)).collectAsList();
				final List<Column> ink = new ArrayList<Column>();
				for(final Object o : keys) {
					ink.add(functions.lit(o));
				}
				dt.delete(functions.col(primaryKey).isin(ink.toArray()));
			}
		}
	}
	
	public void endTableUpdates(final String prefix, final String schema, final String table) {
		final DeltaTable dt = getTable(prefix, schema, table);
		updateManifest(dt);
	}
	
	protected DeltaTable createTable(final Dataset<Row> payload, final String prefix, final String schema, final String table) {
		payload.write().format("delta")
			.mode("overwrite")
			.option("delta.compatibility.symlinkFormatManifest.enabled", true)
			.option("delta.autoOptimize.optimizeWrite", true)
			.option("delta.autoOptimize.autoCompact", true)
			.save(getTablePath(prefix, schema, table));
		// create symlink
		final DeltaTable dt = getTable(prefix, schema, table);
		updateManifest(dt);
		return dt;
	}
	
	protected void updateManifest(final DeltaTable dt) {
		try {
			dt.generate("symlink_format_manifest");
		} catch(Exception e) {
			// why are we here
		}
	}
	
	public boolean exists(final String prefix, final String schema, final String table) {
		return DeltaTable.isDeltaTable(getTablePath(prefix, schema, table));
	}
	

	public Dataset<Row> load(final String prefix, final String schema, final String table) {
		final DeltaTable dt = getTable(prefix, schema, table);
		return dt.toDF();
	}
	
	
	
	protected Map<String,String> getExpression(final String source, final String target, final Dataset<Row> df, final List<String> excludes) {
		final Map<String, String> expression = new HashMap<String,String>();
		for(final String field : df.schema().fieldNames()) {
			if(!excludes.contains(field)) {
				expression.put(source + "." + field, target + "." + field);
			}
		}
		return expression;
	}
	
	protected DeltaTable getTable(final String prefix, final String schema, final String table) {
		if(DeltaTable.isDeltaTable(getTablePath(prefix, schema, table)))
			return DeltaTable.forPath(getTablePath(prefix, schema, table));
		else
			return null;
	}
	
	private String getTablePath(final String prefix, final String schema, final String table) {
		return prefix + "/" + schema + "/" + table;
	}
	
	public static class ObjectRowMapper implements MapFunction<Row, Object>, Serializable {
		private static final long serialVersionUID = -3154350133688913152L;

		@Override
		public Object call(Row value) throws Exception {
			return value.get(0);
		}
		
	}
}
