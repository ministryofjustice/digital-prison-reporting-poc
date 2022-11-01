package uk.gov.justice.dpr.domain.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

// see https://docs.google.com/document/d/1KWEjO4SIE-3LugGLkn4c5mp48SgZwqvqxWC0u4KZTaI/edit?usp=sharing

@JsonIgnoreProperties(ignoreUnknown = true)
public class DomainDefinition {
	private String id;
	private String name;
	private String description;
	private String version;
	private String location;
	
	private Map<String,String> tags;
	
	// permissions
	
	private String owner;
	private String author;
	
	private List<TableDefinition> tables = new ArrayList<TableDefinition>();

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public Map<String, String> getTags() {
		return tags;
	}

	public void setTags(Map<String, String> tags) {
		this.tags = tags;
	}

	public String getOwner() {
		return owner;
	}

	public void setOwner(String owner) {
		this.owner = owner;
	}

	public String getAuthor() {
		return author;
	}

	public void setAuthor(String author) {
		this.author = author;
	}

	public List<TableDefinition> getTables() {
		return tables;
	}

	public void setTables(List<TableDefinition> tables) {
		this.tables = tables;
	}
}
