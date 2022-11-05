package uk.gov.justice.dpr.queue;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

import uk.gov.justice.dpr.cdc.EventConverter;

public class Queue {

	private final String name;
	private final AmazonSQSAsync client;
	
	public Queue(final AmazonSQSAsync client, final String name) {
		this.client = client;
		this.name = name;
	}
	
	// this need improvement as we are deleting messages even if we haven't processed them.
	// but it'll do for now
	public Dataset<Row> getQueuedMessages(final SparkSession spark) {
		try {
			final String url = client.getQueueUrl(name).getQueueUrl();
			final ReceiveMessageRequest req = new ReceiveMessageRequest(url).withWaitTimeSeconds(10).withMaxNumberOfMessages(50);
			
			// read the messages and post to the appropriate classes
			final ReceiveMessageResult result = client.receiveMessage(req);
			final List<Message> messages = result.getMessages();
			System.out.format("Received {} messages on queue {}", messages.size(), name);
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
						System.out.println(e.getMessage());
					}
				};
			}
			return union;
		} catch(Exception e) {
			System.out.println(e.getMessage());
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
					// this could be replaced with S3 streaming
					final String path = "s3://" + enr.getS3().getBucket().getName() + "/" + enr.getS3().getObject().getKey();
					final Dataset<Row> df = spark.read().option("wholetext", true).text(path);
					final String json = df.first().getString(0);
					ByteArrayInputStream bais = new ByteArrayInputStream(json.getBytes());
					batches.add(EventConverter.fromRawDMS_3_4_6(spark, bais));
				} catch(Exception e) {
					// do something - we have missed a batch for some reason
				}
			}
		} catch(Exception e) {
			// ignore the event - it is not for us
		}
		return batches;
	}
}
