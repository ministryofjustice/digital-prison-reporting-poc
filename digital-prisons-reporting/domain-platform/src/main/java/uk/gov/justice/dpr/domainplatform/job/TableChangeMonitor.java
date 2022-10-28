package uk.gov.justice.dpr.domainplatform.job;

import static org.apache.spark.sql.functions.col;

import java.util.List;
import java.util.Set;

import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.DataStreamWriter;

import uk.gov.justice.dpr.cdc.EventConverter;
import uk.gov.justice.dpr.domain.DomainRepository;
import uk.gov.justice.dpr.domain.model.DomainDefinition;
import uk.gov.justice.dpr.domainplatform.domain.DomainExecutor;
import uk.gov.justice.dpr.util.TableListExtractor;
import uk.gov.justice.dpr.util.TableListExtractor.TableTuple;

public class TableChangeMonitor {

	protected DataStreamReader dsr;
	protected String domainRepositoryPath;
	protected String sourcePath;
	protected String targetPath;
	
	public TableChangeMonitor(final String domainRepositoryPath, final String sourcePath, final String targetPath, final DataStreamReader dsr) {
		this.dsr = dsr;
		this.domainRepositoryPath = domainRepositoryPath;
		this.sourcePath = sourcePath;
		this.targetPath = targetPath;
	}
	
	@SuppressWarnings("rawtypes")
	public DataStreamWriter run() {
		return run(dsr);
	}
	
	@SuppressWarnings("rawtypes")
	public DataStreamWriter run(final DataStreamReader in) {
		return run(in.load());
	}
	
	@SuppressWarnings("rawtypes")
	public DataStreamWriter run(final Dataset<Row> df) {
		return df.writeStream().foreachBatch(new TableChangeMonitor.Function(df.sparkSession(), domainRepositoryPath, sourcePath, targetPath));
	}
	
	public class Function implements VoidFunction2<Dataset<Row>, Long> {
		private static final long serialVersionUID = 8028572153253155936L;
		
		public Function(final SparkSession spark, final String domainRepositoryPath, final String sourcePath, final String targetPath) {
			this.sourcePath = sourcePath;
			this.targetPath = targetPath;
			this.repo = new DomainRepository(spark, domainRepositoryPath, domainRepositoryPath);
		}
		
		private DomainRepository repo;
		private String sourcePath;
		private String targetPath;

		@Override
		public void call(Dataset<Row> df_events, Long batchId) throws Exception {
			System.out.println("Running batch " + batchId );
			if(!df_events.isEmpty()) {

				try {
					

					Dataset<Row> df = EventConverter.fromKinesis(df_events);
					// get a list of events
					// first get all control messages and process them
					// PAUSE, RESUME
					// get a list of tables the events relate to
					List<TableTuple> tables = TableListExtractor.extractTableList(df);
					
					// find all domains that depend on the events
					for(final TableTuple table : tables) {
						Set<DomainDefinition> domains = repo.getDomainsForSource(table.asString());
						for(final DomainDefinition domain : domains) {
							// for each, start a new domain incremental update
							final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domain);
							
							// extract events that are for this table onlt
							Dataset<Row> changes = df.filter("(recordType == 'data' and schemaName == '" + table.getSchema() +"' and tableName == '" + table.getTable() + "' and (operation == 'load' or operation == 'insert' or operation == 'update' or operation == 'delete'))")
										.orderBy(col("timestamp"));
			
							Dataset<Row> df_payload = EventConverter.getPayload(changes);
							
							executor.doIncremental(df_payload, table);
			
						}
					}
					
				} catch(Exception e) {
					System.err.println(e.getMessage());
				}
			}
		}
		
	}
	
}
