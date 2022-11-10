package uk.gov.justice.dpr.queue;

import java.io.InputStream;

import org.apache.spark.sql.SparkSession;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.S3Object;

public class MessageFileLoader {
	
	private AmazonS3 client;
	
	public MessageFileLoader(final AmazonS3 client) {
		this.client = client;
	}

	public InputStream getContentFromS3NotificationRecord(final SparkSession spark, final S3EventNotificationRecord enr) {
		try {
			final String bucket = enr.getS3().getBucket().getName();
			final String key = enr.getS3().getObject().getUrlDecodedKey();
			System.out.println("Loading " + bucket + "/" + key + "...");
			final S3Object object = this.client.getObject(bucket, key);
			if(object != null) {
				return object.getObjectContent();
			}
		} finally {			
		}
		return null;
	}
}
