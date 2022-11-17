package uk.gov.justice.dpr.domainplatform.domain;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import uk.gov.justice.dpr.delta.DeltaLakeService;
import uk.gov.justice.dpr.domain.model.DomainDefinition;
import uk.gov.justice.dpr.domain.model.TableDefinition;
import uk.gov.justice.dpr.domain.model.TableDefinition.MappingDefinition;
import uk.gov.justice.dpr.domain.model.TableDefinition.TransformDefinition;
import uk.gov.justice.dpr.domain.model.TableDefinition.ViolationDefinition;
import uk.gov.justice.dpr.util.TableListExtractor.TableTuple;

public class DomainExecutor {

	// core initialised values
	// sourceRootPath
	// targetRootPath
	// domainDefinition
	protected String sourceRootPath;
	protected String targetRootPath;
	protected DomainDefinition domainDefinition;
	
	protected DeltaLakeService deltaService = new DeltaLakeService();
	
	public DomainExecutor(final String sourceRootPath, final String targetRootPath, final DomainDefinition domain) {
		this.sourceRootPath = sourceRootPath;
		this.targetRootPath = targetRootPath;
		this.domainDefinition = domain;
	}
	
	// parameters
	// session
	// df
	// source.table
	
	// call
	// incremental
	public void doIncremental(final Dataset<Row> df, final TableTuple sourceTable) {
		// we don't need the source, we just need to apply the changes to the target
		final List<TableDefinition> tables = getTablesChangedForSourceTable(sourceTable);
		for(final TableDefinition table : tables) {
			
			// (3) run transforms
			// (4) run violations
			// (5) run mappings if available			
			final Dataset<Row> df_target = apply(table, sourceTable, df);
		
			// (6) save materialised view
			final TableInfo targetInfo = TableInfo.create(targetRootPath,  domainDefinition.getName(), table.getName());
			saveIncremental(targetInfo, table.getPrimaryKey(), df_target);
		}
	}
	
	// full
	public void doFull(final TableTuple sourceTable) {
		
		final TableInfo sourceInfo = TableInfo.create(sourceRootPath, sourceTable.getSchema(), sourceTable.getTable());		
		final Dataset<Row> df_source = deltaService.load(sourceInfo.getPrefix(), sourceInfo.getSchema(), sourceInfo.getTable());

		final List<TableDefinition> tables = getTablesChangedForSourceTable(sourceTable);
		for(final TableDefinition table : tables) {
			
			// (3) run transforms
			// (4) run violations
			// (5) run mappings if available
			final Dataset<Row> df_target = apply(table, sourceTable, df_source);
		
			// (6) save materialised view
			final TableInfo targetInfo = TableInfo.create(targetRootPath,  domainDefinition.getName(), table.getName());
			saveFull(targetInfo, df_target);
		}
	}
	
	public void doFull() {
		
		// Replace all tables
		final List<TableDefinition> tables = domainDefinition.getTables();
		for(final TableDefinition table : tables) {
			
			// (3) run transforms
			// (4) run violations
			// (5) run mappings if available
			final Dataset<Row> df_target = apply(table, null, null);
		
			// (6) save materialised view
			final TableInfo targetInfo = TableInfo.create(targetRootPath,  domainDefinition.getName(), table.getName());
			saveFull(targetInfo, df_target);
		}
	}
	
	protected Dataset<Row> apply(final TableDefinition table, final TableTuple sourceTable, final Dataset<Row> df) {
		try {
			
			System.out.println("DomainExecutor::applyTransform(" + table.getName() + ")...");
			// Transform
			final Map<String, Dataset<Row>> refs = this.getAllSourcesForTable(table, sourceTable);
			// Add sourceTable if present
			if(sourceTable != null && df != null) {
				refs.put(sourceTable.asString().toLowerCase(), df);
			}
			System.out.println("'" + table.getName() + "' has " + refs.size() + " references to tables...");
			final Dataset<Row> df_transform = applyTransform(refs, table.getTransform());
			
			System.out.println("DomainExecutor::applyViolations(" + table.getName() + ")...");
			// Process Violations - we now have a subset
			final Dataset<Row> df_postViolations = applyViolations(df_transform, table.getViolations());

			System.out.println("DomainExecutor::applyMappings(" + table.getName() + ")...");
			// Mappings
			final Dataset<Row> df_postMappings = applyMappings(df_postViolations, table.getMapping());

			return df_postMappings;

		} catch(Exception e) {
			System.out.println("DomainExecutor::apply(" + table.getName() + ") failed.");
			handleError(e);
			return df;
		}
		finally {
			System.out.println("DomainExecutor::apply(" + table.getName() + ") completed.");
		}
	}
	
	
	protected Dataset<Row> applyMappings(final Dataset<Row> df, final MappingDefinition mapping) {
		if(mapping != null && mapping.getViewText() != null && !mapping.getViewText().isEmpty()) {
			return df.sqlContext().sql(mapping.getViewText()).toDF();
		}
		return df;
	}
	
	protected Dataset<Row> applyViolations(final Dataset<Row> df, final List<ViolationDefinition> violations) {
		Dataset<Row> working_df = df;
		for(final ViolationDefinition violation : violations) {
			final Dataset<Row> df_violations = working_df.where("not(" + violation.getCheck() + ")").toDF();
			if(!df_violations.isEmpty()) {
				TableInfo info = TableInfo.create(targetRootPath, violation.getLocation(), violation.getName());
				saveViolations(info, df_violations);
				working_df = working_df.except(df_violations);
			}
		}
		return working_df;
	}
	
	protected Dataset<Row> applyTransform(final Dataset<Row> in, final TransformDefinition transform) {
		if(transform != null && transform.getSources() != null && transform.getSources().size() == 1) {
			final Map<String,Dataset<Row>> refs = new HashMap<String, Dataset<Row>>();
			refs.put(transform.getSources().get(0), in);
			return applyTransform(refs, transform);
		} else {
			System.err.println("Transform has more than one source");
			return in;
		}
	}
	
	protected Dataset<Row> applyTransform(final Map<String,Dataset<Row>> dfs, final TransformDefinition transform) {
		final List<String> srcs = new ArrayList<String>();
		SparkSession spark = null;
		try {
			String view = transform.getViewText().toLowerCase();
			boolean incremental = false;
			for(final String source : transform.getSources()) {
				final String src = source.toLowerCase().replace(".","__");
				final Dataset<Row> df_source = dfs.get(source);
				if(df_source != null) {
					df_source.createOrReplaceTempView(src);
					System.out.println("Added view '" + src +"'");
					srcs.add(src);
					if(!incremental && 
							schemaContains(df_source, "_operation") && 
							schemaContains(df_source, "_timestamp")) 
					{
						view = view.replace(" from ", ", " + src +"._operation, " + src + "._timestamp from ");
						incremental = true;
					}
					if(spark == null) {
						spark = df_source.sparkSession();
					}
				}
				view = view.replace(source, src);
			}
			System.out.println("Executing view '" + view + "'...");
			return spark == null ? null : spark.sqlContext().sql(view).toDF();
		} catch(Exception e) {
			handleError(e);
			return null;
		} finally {
			try {
				if(spark != null) {
					for(final String source : srcs) {
						spark.catalog().dropTempView(source);
					}
				}
			}
			catch(Exception e) {
				// continue;
			}
		}
	}
	
	protected void saveViolations(final TableInfo target, final Dataset<Row> df) {
		// save the violations to the specified location
		deltaService.append(target.getPrefix(), target.getSchema(), target.getTable(), df);
		deltaService.endTableUpdates(target.getPrefix(), target.getSchema(), target.getTable());
	}
	
	protected void saveFull(final TableInfo info, final Dataset<Row> df) {
		deltaService.replace(info.getPrefix(), info.getSchema(), info.getTable(), df);
		deltaService.endTableUpdates(info.getPrefix(), info.getSchema(), info.getTable());
	}
	
	protected void saveIncremental(final TableInfo info, final String primaryKey, final Dataset<Row> df) {
		deltaService.merge(info.getPrefix(), info.getSchema(), info.getTable(), primaryKey, df);
	}
	
	protected Map<String, Dataset<Row>> getAllSourcesForTable(final TableDefinition table, final TableTuple exclude) {
		Map<String,Dataset<Row>> fullSources = new HashMap<String,Dataset<Row>>();
		if(table.getTransform() != null && table.getTransform().getSources() != null && table.getTransform().getSources().size() > 0) {
			for( final String source : table.getTransform().getSources()) {
				if(exclude != null && exclude.asString().equalsIgnoreCase(source)) {
					// we already have this table
				} else {
					try {
						TableTuple full = new TableTuple(source);
						final Dataset<Row> df = deltaService.load(sourceRootPath, full.getSchema(), full.getTable());
						if(df == null) {
							System.err.println("Unable to load source '" + source +"' for Table Definition '" + table.getName() + "'");
						} else {
							System.out.println("Loaded source '" + full.asString() +"'.");
							fullSources.put(source.toLowerCase(), df);
						}
					} catch(Exception e) {
						handleError(e);
					}
				}
			}
		}
		return fullSources;
	}
	
	protected boolean schemaContains(final Dataset<Row> df, final String field) {
		return Arrays.<String>asList(df.schema().fieldNames()).contains(field);
	}
	
	protected List<TableDefinition> getTablesChangedForSourceTable(final TableTuple sourceTable) {
		List<TableDefinition> tables = new ArrayList<TableDefinition>();
		for(final TableDefinition table : domainDefinition.getTables()) {
			for( final String source : table.getTransform().getSources()) {
				if(sourceTable != null && sourceTable.asString().equalsIgnoreCase(source)) {
					tables.add(table);
					break;
				}
			}
		}
		return tables;
	}

	protected void handleError(final Exception e) {
		final StringWriter sw = new StringWriter();
		final PrintWriter pw = new PrintWriter(sw);
		e.printStackTrace(pw);
		System.err.print(sw.getBuffer().toString());
	}
	
}
