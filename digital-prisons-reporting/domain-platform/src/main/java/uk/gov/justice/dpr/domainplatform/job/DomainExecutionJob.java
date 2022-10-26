package uk.gov.justice.dpr.domainplatform.job;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.fasterxml.jackson.databind.ObjectMapper;

import uk.gov.justice.dpr.delta.DeltaLakeService;
import uk.gov.justice.dpr.domain.DomainLoader;
import uk.gov.justice.dpr.domain.model.DomainDefinition;
import uk.gov.justice.dpr.domain.model.TableDefinition;
import uk.gov.justice.dpr.domain.model.TableDefinition.MappingDefinition;
import uk.gov.justice.dpr.domain.model.TableDefinition.TransformDefinition;
import uk.gov.justice.dpr.domain.model.TableDefinition.ViolationDefinition;
import uk.gov.justice.dpr.domainplatform.domain.TableInfo;

public class DomainExecutionJob {
	
	protected static final ObjectMapper MAPPER = new ObjectMapper();
	
	protected SparkSession spark;
	protected String domainPath; // path to domain file we are processing
	protected String targetPath; // path to target environment
	protected String domainName;
	protected String sourcePath;
	protected String sourceTable;
	protected String operation; // incremental or refresh
	
	protected DomainLoader domainLoader = null;
	protected DeltaLakeService deltaService = new DeltaLakeService();
	
	public DomainExecutionJob(final SparkSession spark, final String domainPath, final String domainName, final String sourcePath, final String sourceTable, final String targetPath, final String operation) {
		this.spark = spark;
		this.domainPath = domainPath;
		this.domainName = domainName;
		this.sourcePath = sourcePath;
		this.sourceTable = sourceTable;
		this.targetPath = targetPath;
		this.operation = operation;
		this.domainLoader = new DomainLoader(spark, domainPath);
	}

	public void run() {
		
		try {
			// (1) load the domain this relates to
			final DomainDefinition domain = domainLoader.loadDomain();
			
			List<TableDefinition> tables = getTablesChangedForSourceTable(domain, sourceTable);
			
			for(final TableDefinition table : tables) {
				if(isIncremental()) {
					doIncremental(table);
				} else {
					doFull(table);
				}
			}
		
		} catch (Exception e) {
			handleError(e);
			throw new RuntimeException(e);
		} 
	}
	
	protected boolean isIncremental() {
		return "incremental".equals(operation);
	}
	
	protected List<TableDefinition> getTablesChangedForSourceTable(final DomainDefinition domain, final String sourceTable) {
		List<TableDefinition> tables = new ArrayList<TableDefinition>();
		for(final TableDefinition table : domain.getTables()) {
			for( final String source : table.getTransform().getSources()) {
				if(sourceTable != null && sourceTable.equals(source)) {
					tables.add(table);
					break;
				}
			}
		}
		return tables;
	}
	

	protected void doIncremental(final TableDefinition table) {
		// (2) load the appropriate delta change feed table (incremental) or the whole table (refresh/load)
		final TableInfo info = TableInfo.create(sourcePath, sourceTable);
		// here we load the change data feed
		// which we cannot do until Version 2.x is deployed
		final Dataset<Row> df_source = deltaService.load(info.getPrefix(), info.getSchema(), info.getTable());
		
		// run the transforms on the inserts and updates
		
		// (3) run transforms
		// (4) run violations
		// (5) run mappings if available
		final Dataset<Row> df_target = apply(table, sourceTable, df_source);
		
		// we then add the deletes and apply it to the materialized view
		// (6) save materialised view
		saveFull(table, df_target);
	}
	
	protected void doFull(final TableDefinition table) {
		// (2) load the appropriate delta change feed table (incremental) or the whole table (refresh/load)
		final TableInfo info = TableInfo.create(sourcePath, sourceTable);		
		final Dataset<Row> df_source = deltaService.load(info.getPrefix(), info.getSchema(), info.getTable());
		
		// (3) run transforms
		// (4) run violations
		// (5) run mappings if available
		final Dataset<Row> df_target = apply(table, info.getTable(), df_source);
		
		// (6) save materialised view
		saveFull(table, df_target);
	}
	
	protected void saveFull(final TableDefinition table, final Dataset<Row> df) {
		deltaService.replace(targetPath, domainName, table.getName(), df);
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
			spark.catalog().dropTempView(sourceTable);
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
				saveViolations(df_violations, violation.getLocation(), violation.getName());
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
	
	protected void saveViolations(final Dataset<Row> df, final String location, final String name) {
		// save the violations to the specified location
		deltaService.append(targetPath + "/" + location, "violations", name, df);
	}
	
	protected void handleError(final Exception e) {
		final StringWriter sw = new StringWriter();
		final PrintWriter pw = new PrintWriter(sw);
		e.printStackTrace(pw);
		System.err.print(sw.getBuffer().toString());
	}
	

}
