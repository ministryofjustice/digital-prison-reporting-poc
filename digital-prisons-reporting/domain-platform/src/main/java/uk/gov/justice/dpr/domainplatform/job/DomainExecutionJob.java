package uk.gov.justice.dpr.domainplatform.job;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import uk.gov.justice.dpr.domain.DomainLoader;
import uk.gov.justice.dpr.domain.model.DomainDefinition;

public class DomainExecutionJob {
	
	private static final ObjectMapper MAPPER = new ObjectMapper();
	
	private SparkSession spark;
	private String domainPath;
	private String domainName;
	private String sourceTable;
	private String operation; // incremental or refresh
	
	private DomainLoader domainValidator = new DomainLoader();
	
	public DomainExecutionJob(final SparkSession spark, final String domainPath, final String domainName, final String sourceTable, final String operation) {
		this.domainName = domainName;
		this.sourceTable = sourceTable;
		this.operation = operation;
	}

	public void run() {
		
		try {
		// (1) load the domain this relates to
		final DomainDefinition domain = loadDomain();
		
		// (2) load the appropriate delta change feed table (incremental) or the whole table (refresh/load)
		
		// (3) run transforms
		// (4) run violations
		// (5) run mappings if available
		
		// (6) save materialised view
		} catch (JsonMappingException e) {
			handleError(e);
		} catch (JsonProcessingException e) {
			handleError(e);
		} catch (IllegalArgumentException e) {
			handleError(e);
		} finally {}
	}
	
	protected DomainDefinition loadDomain() throws JsonMappingException, JsonProcessingException, IllegalArgumentException {
		final String path = domainPath + domainName;
		final Dataset<Row> df = spark.read().option("wholetext", true).text(path);
		final String json = df.first().getString(0);
		final DomainDefinition def = MAPPER.readValue(json, DomainDefinition.class);
		domainValidator.validate(def);
		return def;
	}
	
	protected void handleError(final Exception e) {
		final StringWriter sw = new StringWriter();
		final PrintWriter pw = new PrintWriter(sw);
		e.printStackTrace(pw);
		System.err.print(sw.getBuffer().toString());
	}
}
