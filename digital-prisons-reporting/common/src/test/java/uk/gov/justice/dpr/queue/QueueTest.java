package uk.gov.justice.dpr.queue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

import uk.gov.justice.dpr.BaseSparkTest;

@RunWith(MockitoJUnitRunner.class)
public class QueueTest extends BaseSparkTest {

	@Mock AmazonSQS client;
	@Mock MessageFileLoader loader;
	
	@Before
	public void before() {
		super.before();
		when(client.getQueueUrl(any(String.class))).thenReturn(new GetQueueUrlResult());
	}

	
	@Test
	public void shouldCreateQueue() {
		final Queue queue = new Queue(client, loader, "queue.name");
		assertNotNull(queue);
	}
	
	@Test
	public void shouldGetAMessageWhenNotificationIsInQueue() throws JsonParseException, JsonMappingException, IOException {
		
		final InputStream f = getStream("/sample/events/raw-on-disk-events.json");
		final String s3n = this.getResource("/sample/events/s3-notification-event");
		
		ReceiveMessageResult result = new ReceiveMessageResult();
		Message message = new Message();
		message.setBody(s3n);
		result.setMessages(Arrays.asList(message));
		
		when(client.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(result);
		when(loader.getContentFromS3NotificationRecord(eq(spark), any())).thenReturn(f);
		
		final Queue queue = new Queue(client, loader, "queue.name");

		Dataset<Row> df = queue.getQueuedMessages(spark);
		
		assertNotNull(df);
		assertFalse(df.isEmpty());
		assertEquals(490, df.count());
		
		verify(client, times(1)).deleteMessage(any());
	}
	
}
