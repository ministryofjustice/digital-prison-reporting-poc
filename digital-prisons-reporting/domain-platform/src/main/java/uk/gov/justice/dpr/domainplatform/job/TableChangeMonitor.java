package uk.gov.justice.dpr.domainplatform.job;

import static org.apache.spark.sql.functions.col;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.Set;

import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.types.DataType;

import uk.gov.justice.dpr.cdc.EventConverter;
import uk.gov.justice.dpr.service.SourceReferenceService;
import uk.gov.justice.dpr.domain.DomainRepository;
import uk.gov.justice.dpr.domain.model.DomainDefinition;
import uk.gov.justice.dpr.domainplatform.domain.DomainExecutor;
import uk.gov.justice.dpr.queue.Queue;
import uk.gov.justice.dpr.util.TableListExtractor;
import uk.gov.justice.dpr.util.TableListExtractor.TableTuple;

public class TableChangeMonitor {

	protected SparkSession spark;
	protected Queue queue;
	protected String domainRepositoryPath;
	protected String sourcePath;
	protected String targetPath;
	
	public TableChangeMonitor(final SparkSession spark, final Queue queue, final String domainRepositoryPath, final String sourcePath, final String targetPath) {
		this.domainRepositoryPath = domainRepositoryPath;
		this.sourcePath = sourcePath;
		this.targetPath = targetPath;
		this.spark = spark;
		this.queue = queue;
	}
	
	public DataStreamWriter<Object> run() {
		Dataset<Row> df = queue.getQueuedMessages(spark);
		if(df != null) {
			try {
				new TableChangeMonitor.Function(spark, domainRepositoryPath, sourcePath, targetPath).call(df, Long.valueOf(0L));
			} catch (Exception e) {
				handleError(e);
			}
		}
		return null;
	}
	
	public class Function implements VoidFunction2<Dataset<Row>, Long> {
		private static final long serialVersionUID = 8028572153253155936L;
		
		public Function(final SparkSession spark, final String domainRepositoryPath, final String sourcePath, final String targetPath) {
			this.sourcePath = sourcePath;
			this.targetPath = targetPath;
			// a read-only repo
			this.repo = new DomainRepository(spark, domainRepositoryPath);
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
					List<TableTuple> tables = TableListExtractor.extractTranslatedTableList(df);
					
					// tables have aliases. So SYSTEM OFFENDERS or OMS_OWNER OFFENDERS actually apply to nomis.offenders
					// we need to translate table schema/table names into 'local' names
					// this is done by the TABLE LIST EXTRACTOR
					System.out.println("Batch contains " + (tables == null ? 0 : tables.size()) + " tables");
					
					// find all domains that depend on the events
					for(final TableTuple table : tables) {
						
						System.out.println("TableChangeMonitor::process(" + table.getSchema() + "." + table.getTable() + ") started...");
						Set<DomainDefinition> domains = repo.getDomainsForSource(table.asString());
						System.out.println("TableChangeMonitor::process(" + table.getSchema() + "." + table.getTable() + ") found " + domains.size() + " domains");
						for(final DomainDefinition domain : domains) {
							// for each, start a new domain incremental update
							System.out.println("TableChangeMonitor::processDomainIncremental(" + domain.getName() +") started...");
							final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domain);
							
							// extract events that are for this table onlt
							Dataset<Row> changes = df.filter("(recordType == 'data' and schemaName == '" + table.getOriginalSchema() +"' and tableName == '" + table.getOriginalTable() + "' and (operation == 'load' or operation == 'insert' or operation == 'update' or operation == 'delete'))")
										.orderBy(col("timestamp"));
			
						    final DataType payloadSchema = SourceReferenceService.getSchema(table.asString());

							Dataset<Row> df_payload = EventConverter.getPayload(changes, payloadSchema);
							
							executor.doIncremental(df_payload, table);
							System.out.println("TableChangeMonitor::processDomainIncremental(" + domain.getName() +") completed.");
						}
						System.out.println("TableChangeMonitor::process(" + table.getSchema() + "." + table.getTable() + ") completed.");
						
					}
					
				} catch(Exception e) {
					System.out.println("TableChangeMonitor::process() failed - " + e.getMessage());
					handleError(e);
				}
			}
		}
	}
	

	protected static void handleError(final Exception e) {
		final StringWriter sw = new StringWriter();
		final PrintWriter pw = new PrintWriter(sw);
		e.printStackTrace(pw);
		System.err.print(sw.getBuffer().toString());
	}
}
