package uk.gov.justice.dpr;



import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.schema_of_json;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;


public abstract class BaseSparkTest {

	@Rule 
	public TemporaryFolder folder = new TemporaryFolder();
	
	protected SparkSession spark;
	

	protected String accessKey;
	protected String secretKey;
	

	@Before
	public void before() {
		
		final AWSCredentials credentials = new ProfileCredentialsProvider("moj").getCredentials();
		accessKey = credentials.getAWSAccessKeyId();
		secretKey = credentials.getAWSSecretKey();

		spark = SparkSession.builder()
			    .appName("test")
			    .enableHiveSupport()
				.config("spark.master", "local")
				// important delta configurations
				// =================================
				// these need to be in the cloud-platform
				.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
			    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
			    .config("spark.databricks.delta.schema.autoMerge.enabled", true)
			    // ============================
			    .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
			    // ============================
			    // these are needed for test but NOT for live
			    // the manifest needs a HiveContext and this handles a separate one for each test
			    // otherwise we do an inmem one : jdbc:derby://localhost:1527/memory:myInMemDB;create=true
			    .config("javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=" + folder.getRoot().getAbsolutePath() + "/metastore_db_test;create=true")
			    .getOrCreate();

		spark.sparkContext().hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true");
		spark.sparkContext().hadoopConfiguration().set("fs.s3a.impl", org.apache.hadoop.fs.s3a.S3AFileSystem.class.getName());
		spark.sparkContext().hadoopConfiguration().set("fs.s3a.access.key", accessKey);
		spark.sparkContext().hadoopConfiguration().set("fs.s3a.secret.key", secretKey);
		spark.sparkContext().hadoopConfiguration().set("fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A");
		
		assertNotNull(spark);
	}

	@After
	public void after() {
		spark.stop();
		// cleanup derby database
	}
	
	// Test
	protected boolean hasField(final Dataset<Row> in, final String fieldname) {
		return Arrays.asList(in.schema().fieldNames()).contains(fieldname);
	}
	
	protected Object getFirstValue(final Dataset<Row> in, final String fieldname) {
		final Row first = in.first();
		return first.get(first.fieldIndex(fieldname));
	}
	
	protected Dataset<Row> getPayload(Dataset<Row> df, final String column, final String parsed) {
		final DataType schema = getSchema(df, column);
		return df.withColumn(parsed, from_json(col(column), schema));
	}
	
	protected DataType getSchema(Dataset<Row> df, final String column) {
		final Row[] schs = (Row[])df.sqlContext().range(1).select(
				schema_of_json(lit(df.select(column).first().getString(0)))
				).collect();
		final String schemaStr = schs[0].getString(0);
		
		return DataType.fromDDL(schemaStr);
	}
	
	
	protected Dataset<Row> loadDataframe(final String resource, final String filename) throws IOException {
		return spark.read().load(createFileFromResource(resource, filename).toString());
	}
	
	protected Dataset<Row> loadCsvDataframe(final String resource, final String filename) throws IOException {
		return spark.read().option("header", "true").format("csv").load(createFileFromResource(resource, filename).toString());
	}
	
	protected Dataset<Row> loadJsonDataframe(final String resource, final String filename) throws IOException {
		return spark.read().option("multiline", "true").format("json").load(createFileFromResource(resource, filename).toString());
	}
	
	protected Dataset<Row> loadParquetDataframe(final String resource, final String filename) throws IOException {
		return spark.read().parquet(createFileFromResource(resource, filename).toString());
	}

	protected Path createFileFromResource(final String resource, final String filename) throws IOException {
		final InputStream stream = System.class.getResourceAsStream(resource);
		final File f = folder.newFile(filename);
		FileUtils.copyInputStreamToFile(stream, f);
		return Paths.get(f.getAbsolutePath());
	}
	
	@SuppressWarnings("deprecation")
	protected String getResource(final String resource) throws IOException {
		final InputStream stream = System.class.getResourceAsStream(resource);
		return IOUtils.toString(stream);
	}
	
	protected boolean areEqual(final Dataset<Row> a, final Dataset<Row> b) {
		if(!a.schema().equals(b.schema()))
			return false;
		final List<Row> al = a.collectAsList();
		final List<Row> bl = b.collectAsList();
		
		if(al == null && bl == null) return true;
		
		if(al.isEmpty() && bl.isEmpty()) return true;
		if(al.isEmpty() && !bl.isEmpty()) return false;
		if(!al.isEmpty() && bl.isEmpty()) return false;
		
		if(CollectionUtils.subtract(al, bl).size() != 0)
			return false;
		
		return true;
	}
}
