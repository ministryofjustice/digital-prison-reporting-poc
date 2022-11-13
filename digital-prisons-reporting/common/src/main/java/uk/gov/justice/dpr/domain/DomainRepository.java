package uk.gov.justice.dpr.domain;

import static org.apache.spark.sql.functions.array_contains;
import static org.apache.spark.sql.functions.col;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
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
	protected final static String SCHEMA = "domain-repository";
	protected final static String TABLE = "domain";

	protected SparkSession spark;
	protected String domainFilesPath; // sourceDomains
	protected String domainRepositoryPath; // delta table repo
	protected DeltaLakeService service = new DeltaLakeService();
	
	
	public DomainRepository(final SparkSession spark, final String domainRepositoryPath) {
		this(spark, null, domainRepositoryPath);
	}
	
	public DomainRepository(final SparkSession spark, final String domainFilesPath, final String domainRepositoryPath) {
		this.spark = spark;
		this.domainFilesPath = domainFilesPath;
		this.domainRepositoryPath = domainRepositoryPath;
		System.out.println("Domain Repository(files='" + this.domainFilesPath + "';repo='" + this.domainRepositoryPath + "')");
	}
	
	public void touch() {
		load();
	}
	
	public boolean exists() {
		return service.exists(domainRepositoryPath, SCHEMA, TABLE);
	}
	
	public Set<DomainDefinition> getDomainsForSource(final String sourceTable) {
		Set<DomainDefinition> domains = new HashSet<DomainDefinition>();
		try {
			// does a table exist on disk
			
			final Dataset<Row> df = getDomainRepository();
			if(df != null) {
				final List<String> results = df
						.where(array_contains(col("sources"), sourceTable))
						.select(col("definition")).as(Encoders.STRING())
						.collectAsList();
		
				for(final String result : results) {
					domains.add(MAPPER.readValue(result, DomainDefinition.class));
				}
			} else {
				throw new RuntimeException("Domain Repository (" + domainRepositoryPath + "/" + SCHEMA + "/" + TABLE + ") does not exist. Please refresh the domain repository");
			}
		} catch(Exception e) {
			handleError(e);
		}
		return domains;
	}
	
	protected Dataset<Row> getDomainRepository() {
		if(service.exists(domainRepositoryPath, SCHEMA, TABLE)) {
			return service.load(domainRepositoryPath, SCHEMA, TABLE);
		}
		return null;
	}
	
	protected void load() {
		// this loads all the domains that are in the the bucket into a repository for referencing
		if(domainFilesPath == null || domainFilesPath.isEmpty()) {
			throw new IllegalArgumentException("No Domain Files Path. The repository is read-only and cannot load domains.");
		}
		
		try {
			final Dataset<Row> df_domains = spark.read().option("wholetext", true).option("recursiveFileLookup", true).text(domainFilesPath);
			
			final List<DomainRepoRecord> records = new ArrayList<DomainRepoRecord>();
			
			final List<Row> listDomains = df_domains.collectAsList();
			
			System.out.println("Processing domains (" + listDomains.size() + "...)");
			for(final Row row : listDomains) {
				loadOne("", row.getString(0), records);
			}
			
			// replace the repository 
			List<Row> rows = new ArrayList<Row>();
			for(final DomainRepoRecord record : records) {
				rows.add(record.toRow());
			}
			
			final Dataset<Row> df = spark.createDataFrame(rows, DomainRepoRecord.SCHEMA);
			if(service.exists(domainFilesPath, SCHEMA, TABLE)) {
				service.replace(domainRepositoryPath, SCHEMA, TABLE, df);
			} else {
				service.append(domainRepositoryPath, SCHEMA, TABLE, df);
			}
		} catch(Exception e) {
			handleError(e);
		}
	}
	
	protected void loadOne(final String filename, final String json, List<DomainRepoRecord> records ) {
		try {
			
			final DomainDefinition domain = MAPPER.readValue(json, DomainDefinition.class);
			System.out.println("Processing domain '" + domain.getName() + "'");
			
			DomainRepoRecord record = new DomainRepoRecord();
			
			record.setActive(true);
			record.setName(domain.getName());
			record.setVersion(domain.getVersion());
			record.setLocation(filename);
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
				.add("sources", new ArrayType(DataTypes.StringType, true))
				.add("definition", DataTypes.StringType);
		
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
