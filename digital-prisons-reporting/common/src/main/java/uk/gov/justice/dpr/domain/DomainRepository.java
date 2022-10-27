package uk.gov.justice.dpr.domain;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import com.fasterxml.jackson.databind.ObjectMapper;

import scala.collection.JavaConverters;
import scala.collection.Seq;
import uk.gov.justice.dpr.delta.DeltaLakeService;
import uk.gov.justice.dpr.domain.model.DomainDefinition;
import uk.gov.justice.dpr.domain.model.TableDefinition;

/**
 * All domains are stored in a repository.
 * As files cannot be properly 'watched' by Spark, all domains are loaded into a DELTA TABLE using a lambda
 * - New files & Updated Files( s3:ObjectCreated:* ) trigger a Lambda that upserts the details into the repository
 * - Deleted Files (s3:ObjectRemoved) trigger a Lambda that deleted the details from the repository
 * 
 * The repository contains:
 * | NAME | VERSION | ACTIVE | LOCATION | SOURCES | DEFINITION |
 * 
 * Name = name of domain (eg safety)
 * Version = version of domain (eg 1.2.3.4)
 * Active = flag whether active or not
 * Location = S3://location/of/domain/file.json for possible reload
 * Sources = array of source table names [source.table, prisons.prisoner, health.profile]
 * Definition = the json definition loaded from the location
 * 
 * Lambda operations:
 * 
 * DomainRepository.upsert(location, definition, timestamp)
 * DomainRepository.delete(location, definition, timestamp)
 * 
 * TableMonitor operations:
 * 
 * DomainRepository.getDomainsForSource(sourceTable, activeOnly)
 * 
 * DomainRepository.rebuild()
 * 
 * As this is shared between both Lambdas and the domain application it is in common
 * 
 * @author dominic.messenger
 *
 */
public class DomainRepository {
	
	private final static ObjectMapper MAPPER = new ObjectMapper();
	private final static String SCHEMA = "domain-repository";
	private final static String TABLE = "domain";

	protected SparkSession spark;
	protected String domainFilesPath; // sourceDomains
	protected String domainRepositoryPath; // delta table repo
	protected DeltaLakeService service = new DeltaLakeService();
		
	public DomainRepository(final SparkSession spark, final String domainFilesPath, final String domainRepositoryPath) {
		this.spark = spark;
		this.domainFilesPath = domainFilesPath;
		this.domainRepositoryPath = domainRepositoryPath;
	}
	
	public void touch() {
		load();
	}
	
	public Set<DomainDefinition> getDomainsForSource(final String sourceTable) {
		return null;
	}
	
	protected void load() {
		// this loads all the domains that are in the repository into the architecture
		final List<String> df_domains = spark.read()
			.option("recursiveFileLookup", true)
			.textFile(domainRepositoryPath).collectAsList();
		
		final List<DomainRepoRecord> records = new ArrayList<DomainRepoRecord>();
		
		for(final String json : df_domains) {
			loadOne(json, records);
		}
		
		// replace the repository 
		List<Row> rows = new ArrayList<Row>();
		for(final DomainRepoRecord record : records) {
			rows.add(record.toRow());
		}
		
		final Dataset<Row> df = spark.createDataFrame(rows, DomainRepoRecord.SCHEMA);
		service.replace(domainRepositoryPath, SCHEMA, TABLE, df);
	}
	
	protected void loadOne(final String json, List<DomainRepoRecord> records ) {
		try {
			final DomainDefinition domain = MAPPER.readValue(json, DomainDefinition.class);
			
			DomainRepoRecord record = new DomainRepoRecord();
			
			record.setActive(true);
			record.setName(domain.getName());
			record.setVersion(domain.getVersion());
			record.setLocation("");
			record.setDefinition(json);
			for(final TableDefinition table : domain.getTables()) {
				for(final String source : table.getTransform().getSources()) {
					record.getSources().add(source);
				}
			}
			
			records.add(record);
			
		} catch (Exception e) {
			handleError(e);
		}
	}


	protected void handleError(final Exception e) {
		final StringWriter sw = new StringWriter();
		final PrintWriter pw = new PrintWriter(sw);
		e.printStackTrace(pw);
		System.err.print(sw.getBuffer().toString());
	}
	
	public static class DomainRepoRecord {
		
		private static StructType SCHEMA = new StructType()
				.add("name", DataTypes.StringType)
				.add("version", DataTypes.StringType)
				.add("active", DataTypes.BooleanType)
				.add("location", DataTypes.StringType)
				.add("sources", new ArrayType(DataTypes.StringType, true));
		
		protected String name;
		protected String version;
		protected boolean active;
		protected String location;
		protected Set<String> sources = new HashSet<String>();
		protected String definition;
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public String getVersion() {
			return version;
		}
		public void setVersion(String version) {
			this.version = version;
		}
		public boolean isActive() {
			return active;
		}
		public void setActive(boolean active) {
			this.active = active;
		}
		public String getLocation() {
			return location;
		}
		public void setLocation(String location) {
			this.location = location;
		}
		public Set<String> getSources() {
			return sources;
		}
		public void setSources(Set<String> sources) {
			this.sources = sources;
		}
		public String getDefinition() {
			return definition;
		}
		public void setDefinition(String definition) {
			this.definition = definition;
		}
		
		public Row toRow() {
			Seq<?> sources = JavaConverters.asScalaIteratorConverter(this.sources.iterator()).asScala().toSeq();
			List<Object> items = Arrays.asList(new Object[] { this.name, this.version, this.active, this.location, sources, this.definition});
			return Row.fromSeq(JavaConverters.asScalaIteratorConverter(items.iterator()).asScala().toSeq());
		}
	}
}
