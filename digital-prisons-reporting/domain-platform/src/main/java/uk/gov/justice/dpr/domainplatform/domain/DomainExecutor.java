package uk.gov.justice.dpr.domainplatform.domain;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

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
		// split it into insert, update and delete groups
		// ordered by timestamp
		// then apply again to DeltaLakeService
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
			final Dataset<Row> df_target = apply(table, sourceTable.getTable(), df_source);
		
			// (6) save materialised view
			final TableInfo targetInfo = TableInfo.create(targetRootPath,  domainDefinition.getName(), table.getName());
			saveFull(targetInfo, df_target);
		}
	}
	
	protected Dataset<Row> apply(final TableDefinition table, final String sourceTable, final Dataset<Row> df) {
		try {
			df.createOrReplaceTempView(sourceTable);

			// Transform
			final Dataset<Row> df_transform = applyTransform(df, table.getTransform());

			// Process Violations - we now have a subset
			final Dataset<Row> df_postViolations = applyViolations(df_transform, table.getViolations());

			// Mappings
			final Dataset<Row> df_postMappings = applyMappings(df_postViolations, table.getMapping());

			return df_postMappings;

		} finally {
			df.sparkSession().catalog().dropTempView(sourceTable);
		}

	}
	
	
	protected Dataset<Row> applyMappings(final Dataset<Row> df, final MappingDefinition mapping) {
		if(mapping.getViewText() != null && !mapping.getViewText().isEmpty()) {
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
	
	protected Dataset<Row> applyTransform(final Dataset<Row> df, final TransformDefinition transform) {
		try {
			if(transform.getViewText() != null && !transform.getViewText().isEmpty()) {
				return df.sqlContext().sql(transform.getViewText()).toDF();
			}
		} catch(Exception e) {
			handleError(e);
		}
		return df;
	}
	
	
	protected void saveViolations(final TableInfo target, final Dataset<Row> df) {
		// save the violations to the specified location
		deltaService.append(target.getPrefix(), target.getSchema(), target.getTable(), df);
	}
	
	protected void saveFull(final TableInfo info, final Dataset<Row> df) {
		deltaService.replace(info.getPrefix(), info.getSchema(), info.getTable(), df);
	}
	
	
	protected List<TableDefinition> getTablesChangedForSourceTable(final TableTuple sourceTable) {
		List<TableDefinition> tables = new ArrayList<TableDefinition>();
		for(final TableDefinition table : domainDefinition.getTables()) {
			for( final String source : table.getTransform().getSources()) {
				if(sourceTable != null && sourceTable.asString().equals(source)) {
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
