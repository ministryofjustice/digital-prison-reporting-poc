package uk.gov.justice.dpr.domainplatform.domain;

import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;

import com.fasterxml.jackson.databind.ObjectMapper;

import uk.gov.justice.dpr.BaseSparkTest;
import uk.gov.justice.dpr.delta.DeltaLakeService;
import uk.gov.justice.dpr.domain.DomainValidator;
import uk.gov.justice.dpr.domain.model.DomainDefinition;
import uk.gov.justice.dpr.domain.model.TableDefinition;
import uk.gov.justice.dpr.domain.model.TableDefinition.MappingDefinition;
import uk.gov.justice.dpr.domain.model.TableDefinition.TransformDefinition;

public abstract class BaseDomainTest extends BaseSparkTest {
	

	protected static final ObjectMapper MAPPER = new ObjectMapper();
	protected final DeltaLakeService service = new DeltaLakeService();
	
	// shouldLoadADomainJsonAndValidate	
	protected DomainDefinition loadAndValidateDomain(final String domainPath) throws Exception {
		final String json = getResource(domainPath);
		final DomainDefinition def = MAPPER.readValue(json, DomainDefinition.class);
		final DomainValidator validator = new DomainValidator();
		validator.validate(def);
		return def;
	}
	
	protected Dataset<Row> applyTransform(final TableDefinition table, Dataset<Row> df) {
		return applyTransform(table.getTransform(), df);
	}
	
	protected Dataset<Row> applyTransform(final TransformDefinition transform, Dataset<Row> df) {
		final Map<String, Dataset<Row>> refs = new HashMap<String,Dataset<Row>>();
		if(transform.getSources().size() == 1) {
			refs.put(transform.getSources().get(0), df);
			return applyTransform(refs, transform);
		} else {
			throw new IllegalArgumentException("Cannot apply transform as there are more than one source");
		}
	}
	
	protected Dataset<Row> applyTransform(final Map<String,Dataset<Row>> dfs, final TransformDefinition transform) {
		final List<String> srcs = new ArrayList<String>();
		SparkSession spark = null;
		try {
			String view = transform.getViewText();
			boolean incremental = false;
			for(final String source : transform.getSources()) {
				final String src = source.replace(".","__");
				final Dataset<Row> df_source = dfs.get(source);
				if(df_source != null) {
					df_source.createOrReplaceTempView(src);
					srcs.add(src);
					if(!incremental && 
							df_source.schema().contains("_operation") && 
							df_source.schema().contains("_timestamp")) 
					{
						view = view.replace(" from ", ", " + src +"._operation, " + src + "._timestamp from ");
						incremental = true;
					}
					if(spark == null) {
						spark = df_source.sparkSession();
					}
				}
				view = view.replace(source, src);
			}
			return spark == null ? null : spark.sqlContext().sql(view).toDF();
		} catch(Exception e) {
			return null;
		} finally {
			try {
				if(spark != null) {
					for(final String source : srcs) {
						spark.catalog().dropTempView(source);
					}
				}
			}
			catch(Exception e) {
				// continue;
			}
		}
	}
	
	protected Dataset<Row> applyMapping(final TableDefinition table, final Dataset<Row> in) {
		try {
			final MappingDefinition mapping = table.getMapping();
			if(mapping != null) {
				String view = mapping.getViewText();
				return in.sqlContext().sql(view).toDF();
			}
			return in;
		} finally {
		}
	}
	
	protected String getType(final Dataset<Row> in, final String name) {
		int index = in.schema().fieldIndex(name);
		if(index >=0 && index < in.schema().fields().length) {
			final StructField field = in.schema().fields()[index];
			return field == null ? null : field.dataType().typeName();
		}
		return null;
	}
	
	protected void saveToDisk(final String schema, final String table, final Dataset<Row> df) {
		final String prefix = folder.getRoot().getAbsolutePath() + "/target";
		service.replace(prefix, schema, table, df);
	}
	
	protected void mergeToDisk(final String schema, final String table, final String primaryKey, final Dataset<Row> df) {
		final String prefix = folder.getRoot().getAbsolutePath() + "/target";
		service.merge(prefix, schema, table, primaryKey, df);
	}
	
	protected Dataset<Row> readFromDisk(final String schema, final String table) {
		final String prefix = folder.getRoot().getAbsolutePath() + "/target";
		return service.load(prefix, schema, table);
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
