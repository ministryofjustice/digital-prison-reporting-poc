package uk.gov.justice.dpr.domainplatform.domain;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.sql.SparkSession;

import com.fasterxml.jackson.databind.ObjectMapper;

import uk.gov.justice.dpr.domain.model.DomainDefinition;
import uk.gov.justice.dpr.domain.model.TableDefinition;

/**
 * All domains are in a repository : S3, or file system in an hierarchy that is partitioned for 
 * ease of use not for repository processing.
 * A repository loads all the domains using Spark to get the 'rows' (files) and
 * 
 * Uses spark.read.option("recursiveFileLookup", true).textFile(s3://some-bucket/*")
 * 
 * @author dominic.messenger
 *
 */
public class DomainRepository {
	
	private final static ObjectMapper MAPPER = new ObjectMapper();

	protected SparkSession spark;
	protected String domainRepositoryPath;
	

	protected Map<String, DomainDefinition> domainNameMap = new HashMap<String, DomainDefinition>();
	protected Map<String, Set<DomainDefinition>> sourceTableMap = new HashMap<String, Set<DomainDefinition>>();
	
	public DomainRepository(final SparkSession spark, final String domainRepositoryPath) {
		this.spark = spark;
		this.domainRepositoryPath = domainRepositoryPath;
	}
	
	public void touch() {
		load();
	}
	
	public Set<DomainDefinition> getDomainsForSource(final String sourceTable) {
		final  Set<DomainDefinition>  domains = sourceTableMap.get(sourceTable);
		return domains == null ? Collections.<DomainDefinition>emptySet() : domains;
	}
	
	protected void load() {
		// this loads all the domains that are in the repository into the architecture
		final List<String> df_domains = spark.read()
			.option("recursiveFileLookup", true)
			.textFile(domainRepositoryPath).collectAsList();
		
		final Map<String, DomainDefinition> domains = new HashMap<String, DomainDefinition>();
		final Map<String, Set<DomainDefinition>> sources = new HashMap<String, Set<DomainDefinition>>();
		
		for(final String json : df_domains) {
			loadOne(json, domains, sources);
		}
		
		// replace the current ones
		this.domainNameMap = domains;
		this.sourceTableMap = sources;
	}
	
	protected void loadOne(final String json, final Map<String, DomainDefinition> domains, final Map<String, Set<DomainDefinition>> sources ) {
		try {
			final DomainDefinition domain = MAPPER.readValue(json, DomainDefinition.class);
			// patch it in by domain name, tables map
			domains.put(domain.getName(), domain);
			// patch it in by source table
			for(final TableDefinition table : domain.getTables()) {
				for(final String source : table.getTransform().getSources()) {
					patchIntoSourceTableMap(sources, source, domain);
				}
			}
			
		} catch (Exception e) {
			handleError(e);
		}
	}
	
	protected void patchIntoSourceTableMap(final Map<String, Set<DomainDefinition>> sources, final String sourceTable, final DomainDefinition domain) {
		if(sources.containsKey(sourceTable)) {
			sources.get(sourceTable).add(domain);
		} else {
			Set<DomainDefinition> domains = new HashSet<DomainDefinition>();
			domains.add(domain);
			sources.put(sourceTable, domains);
		}
	}


	protected void handleError(final Exception e) {
		final StringWriter sw = new StringWriter();
		final PrintWriter pw = new PrintWriter(sw);
		e.printStackTrace(pw);
		System.err.print(sw.getBuffer().toString());
	}
}
