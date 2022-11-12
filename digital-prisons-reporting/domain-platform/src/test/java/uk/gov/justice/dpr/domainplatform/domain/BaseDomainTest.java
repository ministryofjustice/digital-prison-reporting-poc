package uk.gov.justice.dpr.domainplatform.domain;

import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.fasterxml.jackson.databind.ObjectMapper;

import uk.gov.justice.dpr.BaseSparkTest;
import uk.gov.justice.dpr.delta.DeltaLakeService;
import uk.gov.justice.dpr.domain.DomainValidator;
import uk.gov.justice.dpr.domain.model.DomainDefinition;
import uk.gov.justice.dpr.domain.model.TableDefinition;
import uk.gov.justice.dpr.domain.model.TableDefinition.TransformDefinition;

public abstract class BaseDomainTest extends BaseSparkTest {
	

	protected static final ObjectMapper MAPPER = new ObjectMapper();
	
	// shouldLoadADomainJsonAndValidate	
	protected DomainDefinition loadAndValidateDomain(final String domainPath) throws Exception {
		final String json = getResource(domainPath);
		final DomainDefinition def = MAPPER.readValue(json, DomainDefinition.class);
		final DomainValidator validator = new DomainValidator();
		validator.validate(def);
		return def;
	}
	
	protected Dataset<Row> applyTransform(final TableDefinition table, final Dataset<Row> in) {
		final List<String> srcs = new ArrayList<String>();
		try {
			final TransformDefinition transform = table.getTransform();
			String view = transform.getViewText();
			for(final String source : transform.getSources()) {
				final String src = source.replace(".","__");
				in.createOrReplaceTempView(src);
				srcs.add(src);
				view = view.replace(source, src);
			}
			return in.sqlContext().sql(view).toDF();
		} finally {
			try {
				for(final String source : srcs) {
					spark.catalog().dropTempView(source);
				}
			}
			catch(Exception e) {
				// continue;
			}
		}
	}
	
	protected TableDefinition getTableByName(final DomainDefinition domain, final String name) {
		for(final TableDefinition table : domain.getTables()) {
			if(table.getName().equalsIgnoreCase(name)) {
				return table;
			}
		}
		return null;
	}
	
	protected boolean hasColumn(final Dataset<Row> df, final String name) {
		assertNotNull(df);
		assertNotNull(df.schema());
		for(final String field : df.schema().fieldNames()) {
			if(field.equalsIgnoreCase(name)) return true;
		}
		return false;
	}
	
	protected int columnCount(final Dataset<Row> df) {
		assertNotNull(df);
		assertNotNull(df.schema());
		return df.schema().fields().length;
	}
	
}