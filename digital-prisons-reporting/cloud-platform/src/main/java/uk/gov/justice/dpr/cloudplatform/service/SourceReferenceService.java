package uk.gov.justice.dpr.cloudplatform.service;

import java.util.HashMap;
import java.util.Map;

public class SourceReferenceService {

	
	private static final Map<String, SourceReference> REF = new HashMap<String,SourceReference>();
	
	/**
	 * For the PoC we will hardcode the references - although we would like there to be something
	 * proper in place in future, as we onboard each service.
	 */
	static {
		// demo
		REF.put("system.offenders", new SourceReference("SYSTEM.OFFENDERS", "nomis", "offenders", "OFFENDER_ID"));
		REF.put("system.offender_bookings", new SourceReference("SYSTEM.OFFENDER_BOOKINGS", "nomis", "offender_bookings", "OFFENDER_BOOK_ID"));
		// t3
		REF.put("oms_owner.offenders", new SourceReference("SYSTEM.OFFENDERS", "nomis", "offenders", "OFFENDER_ID"));
		REF.put("oms_owner.offender_bookings", new SourceReference("SYSTEM.OFFENDER_BOOKINGS", "nomis", "offender_bookings", "OFFENDER_BOOK_ID"));
		
		// use of force
		Map<String,String> casts = new HashMap<String,String>();
		casts.put("submitted_date", "date");
		casts.put("created_date", "date");
		casts.put("updated_date", "date");
		casts.put("next_reminder_date", "date");
		casts.put("overdue_date", "date");
		casts.put("deleted", "date");
		casts.put("removal_requested", "date");
		
		REF.put("public.statement", new SourceReference("public.statement", "use_of_force", "statement", "id", casts));
		
		casts = new HashMap<String,String>();
		casts.put("submitted_date", "date");
		casts.put("created_date", "date");
		casts.put("incident_date", "date");
		casts.put("updated_date", "date");
		casts.put("deleted", "date");
		REF.put("public.report", new SourceReference("public.report", "use_of_force", "report", "id", casts));
	}
	
	
	public static String getPrimaryKey(final String key) {
		final SourceReference ref = REF.get(key.toLowerCase());
		return (ref == null ? null : ref.getPrimaryKey());
	}
	
	public static String getSource(final String key) {
		final SourceReference ref = REF.get(key.toLowerCase());
		return (ref == null ? null : ref.getSource());
	}
	
	public static String getTable(final String key) {
		final SourceReference ref = REF.get(key.toLowerCase());
		return (ref == null ? null : ref.getTable());
	}
	
	public static Map<String,String> getCasts(final String key) {
		final SourceReference ref = REF.get(key.toLowerCase());
		return (ref == null ? null : ref.getCasts());
	}
	
	public static class SourceReference {
		
		private String key;
		private String source;
		private String table;
		private String primaryKey;
		private Map<String,String> casts;
		
		public SourceReference(final String key, final String source, final String table, final String primaryKey) {
			this.key = key;
			this.source = source;
			this.table = table;
			this.primaryKey = primaryKey;
			this.casts = new HashMap<String,String>();
		}
		
		public SourceReference(final String key, final String source, final String table, final String primaryKey, Map<String,String> casts) {
			this.key = key;
			this.source = source;
			this.table = table;
			this.primaryKey = primaryKey;
			this.casts = casts;
		}
		
		public String getKey() {
			return key;
		}
		public void setKey(String key) {
			this.key = key;
		}
		public String getSource() {
			return source;
		}
		public void setSource(String source) {
			this.source = source;
		}
		public String getTable() {
			return table;
		}
		public void setTable(String table) {
			this.table = table;
		}
		public String getPrimaryKey() {
			return primaryKey;
		}
		public void setPrimaryKey(String primaryKey) {
			this.primaryKey = primaryKey;
		}
		public Map<String, String> getCasts() {
			return casts;
		}
		public void setCasts(Map<String, String> casts) {
			this.casts = casts;
		}
	}
}
