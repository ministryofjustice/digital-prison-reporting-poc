package uk.gov.justice.dpr.domain;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import uk.gov.justice.dpr.domain.model.DomainDefinition;

/**
 * Role is to load all domains from a particular location; or to load a single domain
**/
public class DomainLoader {
	protected static final ObjectMapper MAPPER = new ObjectMapper();
	
	protected String domainPath;
	protected SparkSession spark;
	
	protected DomainValidator domainValidator = new DomainValidator();
	
	public DomainLoader(final SparkSession spark, final String domainPath) {
		this.spark = spark;
		this.domainPath = domainPath;
	}
	
	
	public DomainDefinition loadDomain() throws JsonMappingException, JsonProcessingException, IllegalArgumentException {
		final String path = domainPath;
		final Dataset<Row> df = spark.read().option("wholetext", true).text(path);
		final String json = df.first().getString(0);
		final DomainDefinition def = MAPPER.readValue(json, DomainDefinition.class);
		domainValidator.validate(def);
		return def;
	}
}
