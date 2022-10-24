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
		
		return null;
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
