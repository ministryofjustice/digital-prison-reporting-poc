package uk.gov.justice.dpr.application.query;

import java.util.HashMap;
import java.util.Map;

public class QueryStore {

	//
	//	SELECT 
	//    date_trunc('week',date(incident_date)) as week,
	//    count(id) as count
	//    FROM domain.incident
	//    WHERE date(incident_date) >= dateadd(day, -28, current_date) 
	//    AND date(incident_date) < current_date
	//    GROUP BY 1
	//    ORDER BY WEEK DESC
	private static final String UOF_INCIDENT_SQL= "select "
			+ "date_trunc('week', date(incident_date)) as week, "
			+ "count(id) as count from mv_aged_incident "
			+ "group by 1 "
			+ "order by week desc";
	
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
