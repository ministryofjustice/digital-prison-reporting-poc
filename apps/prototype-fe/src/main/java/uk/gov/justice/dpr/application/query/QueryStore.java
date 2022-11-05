package uk.gov.justice.dpr.application.query;

import java.util.HashMap;
import java.util.Map;

public class QueryStore {

	// date range query - which for redshift is a two parter:
	// min of range
	// max of range
	private static final String UOF_INCIDENT_SQL=
			"SELECT date_trunc('week',incident_date) as week, "
			+ "count(id) as count FROM use_of_force.incident "
			+ "WHERE incident_date >= dateadd(day, -14, current_date) "
			+ "AND incident_date < current_date "
			+ "GROUP BY 1 "
			+ "ORDER BY week DESC";
	
	private static Map<String,String> queries = new HashMap<String,String>();
	
	static {
		queries.put("use-of-force", UOF_INCIDENT_SQL);
	}

	public static Map<String,String> allQueries() {
		return queries;
	}
	
	public static String getQuery(final String name) {
		return queries.get(name);
	}
}
