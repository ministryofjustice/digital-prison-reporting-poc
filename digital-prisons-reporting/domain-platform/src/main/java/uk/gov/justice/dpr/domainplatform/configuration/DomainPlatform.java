package uk.gov.justice.dpr.domainplatform.configuration;

import java.util.Map;

import org.apache.spark.sql.SparkSession;

import uk.gov.justice.dpr.domainplatform.job.DomainExecutionJob;

public class DomainPlatform {

	
	public static DomainExecutionJob initialise(final SparkSession spark, final Map<String,String> params ) {
		
		if(params == null || params.isEmpty()) {
			throw new IllegalArgumentException("No Parameters provided");
		}
		
		if(spark == null) {
			throw new IllegalArgumentException("Spark Session is null");
		}
		
		final String domainPath = getRequiredParameter(params, "domain.path");
		final String domain = getRequiredParameter(params, "domain.name");
		final String sourcePath = getRequiredParameter(params, "source.path");
		final String sourceTable = getRequiredParameter(params, "source.table");
		final String targetPath = getRequiredParameter(params, "target.path");
		final String operation = getRequiredParameter(params, "domain.operation"); // incremental or full
		
		if(!(operation.toLowerCase().equals("incremental") || operation.toLowerCase().equals("full"))) {
			throw new IllegalArgumentException("Operation must be 'incremental' or 'full'");
		}
		
		return new DomainExecutionJob(spark, domainPath, domain, sourcePath, sourceTable, targetPath, operation);
	}
	
	protected static String getRequiredParameter(final Map<String, String> params, final String name) {
		final String value = params.getOrDefault(name, null);
		if(value == null || value.isEmpty()) 
			throw new IllegalArgumentException(name + " is a required parameter and is missing");
		
		return value;
	}
	
	protected static String getOptionalParameter(final Map<String, String> params, final String name) {
		return params.getOrDefault(name, null);
	}
}
