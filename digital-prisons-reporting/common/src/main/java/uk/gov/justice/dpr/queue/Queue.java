package uk.gov.justice.dpr.queue;

import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

import uk.gov.justice.dpr.cdc.EventConverter;

public class Queue {

	private final String name;
	private final MessageFileLoader loader;
	private final AmazonSQS client;
	
	public Queue(final AmazonSQS client, final MessageFileLoader loader, final String name) {
		this.client = client;
		this.loader = loader;
		this.name = name;
	}
	
	// this need improvement as we are deleting messages even if we haven't processed them.
	// but it'll do for now
	public Dataset<Row> getQueuedMessages(final SparkSession spark) {
		try {
			final String url = client.getQueueUrl(name).getQueueUrl();
			System.out.println("Reading queue '" + name + "'...");
			final ReceiveMessageRequest req = new ReceiveMessageRequest(url).withWaitTimeSeconds(10);
			
			// read the messages and post to the appropriate classes
			final ReceiveMessageResult result = client.receiveMessage(req);
			final List<Message> messages = result.getMessages();
			System.out.println("Received " + messages.size() + " messages on queue " + name);
			Dataset<Row> union = null;
			
			if(messages != null) {
				for(final Message message : messages) {
					try {
						// convert to a dataframe
						List<Dataset<Row>> batches = getDataFrameFromReferencedFile(spark, message);
						for(final Dataset<Row> batch : batches) {
							if(union == null) {
								union = batch;
							} else {
								union = union.unionAll(batch);
							}
						}
						client.deleteMessage(new DeleteMessageRequest(url, message.getReceiptHandle()));
					} catch(Exception e) {
						handleError(e);
					}
				};
			}
			return union;
		} catch(Exception e) {
			handleError(e);
		}
		return null;
	}
	
	private List<Dataset<Row>> getDataFrameFromReferencedFile(final SparkSession spark, final Message message) {
		List<Dataset<Row>> batches = new ArrayList<Dataset<Row>>();
		try {			
			// may be S3Notification
			final S3EventNotification notification = S3EventNotification.parseJson(message.getBody());
			if(notification.getRecords() == null) {
				throw new Exception("Not an S3 Event Notification");
			}
			// get the file using spark wholetextfiles on the exact path
			for(final S3EventNotificationRecord enr : notification.getRecords()) {
				try {
					InputStream bais = loader.getContentFromS3NotificationRecord(spark, enr);
					batches.add(EventConverter.fromRawDMS_3_4_6(spark, bais));
				} catch(Exception e) {
					// do something - we have missed a batch for some reason
					e.printStackTrace();
				}
			}
		} catch(Exception e) {
			// ignore the event - it is not for us
		}
		return batches;
	}
	
	protected void handleError(final Exception e) {
		final StringWriter sw = new StringWriter();
		final PrintWriter pw = new PrintWriter(sw);
		e.printStackTrace(pw);
		System.err.print(sw.getBuffer().toString());
	}
}
