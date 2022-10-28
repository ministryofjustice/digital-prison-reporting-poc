package uk.gov.justice.dpr.domainplatform.job;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.execution.streaming.MemoryStream;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import scala.Option;
import scala.collection.JavaConverters;
import uk.gov.justice.dpr.BaseSparkTest;
import uk.gov.justice.dpr.cdc.KinesisEvent;
import uk.gov.justice.dpr.delta.DeltaLakeService;
import uk.gov.justice.dpr.domain.DomainRepository;
import uk.gov.justice.dpr.domain.model.DomainDefinition;

@RunWith(BlockJUnit4ClassRunner.class)
public class TableChangeMonitorTest extends BaseSparkTest {

	
	@Test
	public void shouldCreateTableChangeMonitor() {
		final TableChangeMonitor monitor = new TableChangeMonitor("domain.repo.path", "source.path", "target.path", null);
		assertNotNull(monitor);
	}
	
	@Test
	public void shouldRunTableChangeMonitor() throws IOException, TimeoutException {
		final String domainSourcePath = folder.getRoot().getAbsolutePath() + "/domains";
		final String domainRepoPath = folder.getRoot().getAbsolutePath() + "/repo";
		final String targetPath = folder.getRoot().getAbsolutePath() + "/target";

		// create some domains
		final DomainRepository repo = createAndFillDomainRepository(domainSourcePath, domainRepoPath, "/sample/domain/domain-system-offenders.json", "/sample/domain/domain-3-tables.json");
		
		Set<DomainDefinition> domains = repo.getDomainsForSource("SYSTEM.OFFENDERS");
		assertNotNull(domains);
		assertFalse(domains.isEmpty());
		
		// run the monitor
		final TableChangeMonitor monitor = new TableChangeMonitor(domainRepoPath, domainSourcePath, targetPath, null);		
		final MemoryStream<KinesisEvent> stream = createAndFillStream(getValidDataset());		
		final StreamingQuery query = monitor.run(stream.toDF()).start();
		query.processAllAvailable();
		
		// check what is there : a single domain table/view materialized
		final DeltaLakeService service = new DeltaLakeService();
		final Dataset<Row> df_result = service.load(targetPath, "example", "prisoner");
		
		assertNotNull(df_result);
		assertFalse(df_result.isEmpty());
		
	}
	
	private DomainRepository createAndFillDomainRepository(final String source, final String repo, final String... domains) throws IOException {
		createDomainSourceFolder("domains", domains);
		
		final DomainRepository domainRepo = new DomainRepository(spark, source, repo);
		domainRepo.touch();
		
		return domainRepo;
	}
	
	private MemoryStream<KinesisEvent> createAndFillStream(Dataset<Row> df) {
		Encoder<KinesisEvent> encoder = Encoders.bean(KinesisEvent.class);
		Option<Object> partition = Option.apply(1);
		
		MemoryStream<KinesisEvent> stream = new MemoryStream<KinesisEvent>(1, spark.sqlContext(), partition, encoder);
		// load some parquet, select the appropriate columns
		Dataset<KinesisEvent> df_events = df.as(encoder);
		// add the data to the stream
		stream.addData(JavaConverters.asScalaIteratorConverter(df_events.collectAsList().iterator()).asScala().toSeq());
		
		return stream;
	}

	private void createDomainSourceFolder(final String source, final String... domains ) throws IOException {
		try {
			folder.newFolder("domains");
		} catch (IOException e) {
			e.printStackTrace();
		}
		for(final String domain : domains) {
			// load the domain from resource
			final String filename = "domain-" + ThreadLocalRandom.current().nextInt(1, 9999999);
			this.createFileFromResource(domain, filename, source);
		}
	}
	private Dataset<Row> getValidDataset() throws IOException {
		final Dataset<Row> df = this.loadParquetDataframe("/sample/events/updates.parquet", "updates.parquet");
		return df.select("data", "streamName", "partitionKey", "sequenceNumber", "approximateArrivalTimestamp");
	}
}
