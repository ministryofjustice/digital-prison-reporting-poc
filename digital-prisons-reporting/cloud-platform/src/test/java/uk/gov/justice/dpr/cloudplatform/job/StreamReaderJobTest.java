package uk.gov.justice.dpr.cloudplatform.job;

import static org.apache.spark.sql.functions.lit;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.kinesis.KinesisSink;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import uk.gov.justice.dpr.BaseSparkTest;
import uk.gov.justice.dpr.cloudplatform.zone.CuratedZone;
import uk.gov.justice.dpr.cloudplatform.zone.RawZone;
import uk.gov.justice.dpr.cloudplatform.zone.StructuredZone;

@RunWith(MockitoJUnitRunner.class)
public class StreamReaderJobTest extends BaseSparkTest {

	@Mock RawZone raw;
	@Mock StructuredZone structured;
	@Mock CuratedZone curated;
	@Mock KinesisSink sink;
	
	
	@Test
	public void shouldCreateJob() {
		final BaseReportingHubJob job = new StreamReaderJob(null, raw, structured, curated, sink);
		
		assertNotNull(job);
	}
	
	@SuppressWarnings({ "rawtypes"})
	@Test
	public void shouldExecuteLoadJob() throws IOException, TimeoutException {
		
		
		final String path = folder.getRoot().getAbsolutePath() ;
		
		// create a parquet file
		final Dataset<Row> df = getEvent("/sample/events/1961-load-event.json");
		final Path resource = this.createFileFromResource("/sample/events/kinesis.parquet", Math.random() + "kinesis.parquet");
		final DataStreamReader dsr = spark.readStream().format("parquet").option("path", resource.toString()).schema(df.schema());
		final Dataset<Row> in = dsr.load();
		final BaseReportingHubJob job = new StreamReaderJob(dsr, raw, structured, curated, sink);
		final DataStreamWriter writer = job.run(dsr)
        // .trigger(Trigger.ProcessingTime("1 seconds"))
        .option("checkpointLocation", path + "/checkpoint/");
		
		// write the data
		
		final StreamingQuery query = writer.start();
		in.unionAll(df);
		try {
			query.awaitTermination();
		} catch(Exception e) {
			
		}
		
		// check that stuff worked
		/**
		verify(raw, times(1)).writeBatch(any(Dataset.class), any(Long.class));
		verify(structured, times(1)).writeBatch(any(Dataset.class), any(Long.class));
		verify(sink, times(1)).open(any(), any());
		verify(sink, times(1)).close(any());
		verify(sink, atLeast(1)).process(any());
		**/
	}
	
	
	protected Dataset<Row> getEvent(final String eventToLoad) throws IOException {
		final String json = this.getResource(eventToLoad);
		return this.loadParquetDataframe("/sample/events/kinesis.parquet", Math.random() + "kinesis.parquet").withColumn("data", lit(json));
	}
}
