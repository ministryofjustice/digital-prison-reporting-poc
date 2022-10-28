package uk.gov.justice.dpr.domain;

import uk.gov.justice.dpr.domain.model.DomainDefinition;
import uk.gov.justice.dpr.domain.model.TableDefinition;

public class DomainValidator {

	public void validate(final DomainDefinition domain) {
		// domain 
		
		// must have an id
		isRequired(domain.getId(), "Domain Id is missing");
		
		// must have a name
		isRequired(domain.getName(), "Domain Name is missing");
		// name must be in a-z and _ only
		isFormatted(domain.getName(), "^[a-z0-9_]+$", "Domain Name does not match format");
		
		// must have a version and it being semantically correct
		isRequired(domain.getVersion(), "Domain Version is missing");
		isFormatted(domain.getVersion(), "^(\\d+\\.)?(\\d+\\.)?(\\d+\\.)?(\\*|\\d+)$", "Domain Version is not formatted correctly");
		
		// must have at least one table
		isTrue(domain.getTables() != null && domain.getTables().size() > 0, "Domain must have at least one table");
		
		for(final TableDefinition table : domain.getTables()) {
			validate(table);
		}
	}
	
	public void validate(final TableDefinition table) {
		// must have a name
		isRequired(table.getName(), "Table Name is missing");
		// name must be in a-z and _ only
		isFormatted(table.getName(), "^[a-z0-9_]+$", "Table '" + table.getName() + "' does not match format");
		// must have a version and it being semantically correct
		isRequired(table.getVersion(), "Table '" + table.getName() + "' Version is missing");
		isFormatted(table.getVersion(), "^(\\d+\\.)?(\\d+\\.)?(\\d+\\.)?(\\*|\\d+)$", "Table '" + table.getName() + "' Version is not formatted correctly");
		// must have a primary key (or does it - can this be optional ?)
		isRequired(table.getPrimaryKey(), "Primary Key is missing");
		// must have a transform
		isRequired(table.getTransform(), "Table '" + table.getName() + "' Transform is missing");
		// transform must map to a source table
		isTrue(table.getTransform().getSources() != null && table.getTransform().getSources().size() > 0, "Table '" + table.getName() + "' must have at least one source table in the transform");
	}
	
	protected void isTrue(final boolean value, final String message) {
		if(!value) {
			throw new IllegalArgumentException(message);
		}
	}
	
	protected void isRequired(final Object value, final String message) {
		if(value == null || (value instanceof String && ((String)value).isEmpty())) {
			throw new IllegalArgumentException(message);
		}
	}
	
	protected void isFormatted(final String value, final String regex, final String message) {
		if(value != null && value.matches(regex)) {
			return;
		}
		throw new IllegalArgumentException(message);
	}
}
