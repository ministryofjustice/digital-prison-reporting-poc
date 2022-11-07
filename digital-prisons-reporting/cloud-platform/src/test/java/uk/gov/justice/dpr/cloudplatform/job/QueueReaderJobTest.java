package uk.gov.justice.dpr.cloudplatform.job;

import static org.apache.spark.sql.functions.lit;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.kinesis.KinesisSink;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

import uk.gov.justice.dpr.BaseSparkTest;
import uk.gov.justice.dpr.cloudplatform.zone.CuratedZone;
import uk.gov.justice.dpr.cloudplatform.zone.RawZone;
import uk.gov.justice.dpr.cloudplatform.zone.StructuredZone;
import uk.gov.justice.dpr.queue.MessageFileLoader;
import uk.gov.justice.dpr.queue.Queue;

@RunWith(MockitoJUnitRunner.class)
public class QueueReaderJobTest extends BaseSparkTest {

	@Mock RawZone raw;
	@Mock StructuredZone structured;
	@Mock CuratedZone curated;
	@Mock KinesisSink sink;

	@Mock AmazonSQSAsync client;
	@Mock MessageFileLoader loader;
	
	
	@Before
	public void before() {
		super.before();
		when(client.getQueueUrl(any(String.class))).thenReturn(new GetQueueUrlResult());
		
	}
	
	@Test
	public void shouldCreateJob() {
		final BaseReportingHubJob job = new QueueReaderJob(spark, null, raw, structured, curated, sink);
		
		assertNotNull(job);
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked"})
	@Test
	public void shouldExecuteLoadJob() throws IOException, TimeoutException {
		
		
		final String path = folder.getRoot().getAbsolutePath() ;
		
		final Queue queue = prepareQueue();
		
		// create a parquet file
		final QueueReaderJob job = new QueueReaderJob(spark, queue, raw, structured, curated, sink);
		final DataStreamWriter writer = job.run();
		
		if(writer != null) {
			writer
	        .trigger(Trigger.Once())
	        .option("checkpointLocation", path + "/checkpoint/");
			
			// write the data
			final StreamingQuery query = writer.start();
			try {
				query.awaitTermination();
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
		
		verify(raw, times(1)).writeBatch(any(Dataset.class), any(Long.class));
		verify(structured, times(1)).writeBatch(any(Dataset.class), any(Long.class));
		verify(curated, times(1)).writeBatch(any(Dataset.class), any(Long.class));
		verify(sink, times(1)).addBatch(any(Long.class), any(Dataset.class));
	}
	
	
	protected Queue prepareQueue() throws IOException {
		final InputStream f = getStream("/sample/events/raw-on-disk-events.json");
		final String s3n = this.getResource("/sample/events/s3-notification-event");
		
		ReceiveMessageResult result = new ReceiveMessageResult();
		Message message = new Message();
		message.setBody(s3n);
		result.setMessages(Arrays.asList(message));
		
		when(client.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(result);
		when(loader.getContentFromS3NotificationRecord(eq(spark), any())).thenReturn(f);
		
		final Queue queue = new Queue(client, loader, "queue.name");

		return queue;
	}
	
	protected Dataset<Row> getEvent(final String eventToLoad) throws IOException {
		final String json = this.getResource(eventToLoad);
		return this.loadParquetDataframe("/sample/events/kinesis.parquet", Math.random() + "kinesis.parquet").withColumn("data", lit(json));
	}
}
