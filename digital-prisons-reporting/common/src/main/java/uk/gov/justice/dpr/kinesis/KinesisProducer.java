package uk.gov.justice.dpr.kinesis;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;

public class KinesisProducer {

	protected static Map<KinesisConfiguration, KinesisProducer> CACHE = new HashMap<KinesisConfiguration, KinesisProducer>();
	
	protected AmazonKinesis client;
	protected KinesisConfiguration config;
	protected String streamName;
	
	public static KinesisProducer getOrCreate(final KinesisConfiguration config) {
		if(CACHE.containsKey(config)) {
			return CACHE.get(config);
		}
		
		final KinesisProducer producer = create(config);
		CACHE.put(config, producer);
		return producer;
	}
	
	protected static KinesisProducer create(final KinesisConfiguration config) {
		
		AmazonKinesisClientBuilder builder = AmazonKinesisClient.builder();
		if(config.getRegion() != null) {
			builder.setRegion(config.getRegion());
		}
		if(config.getCredentialsProvider() != null) {
			builder.setCredentials(config.getCredentialsProvider());
		}
		KinesisProducer producer = new KinesisProducer();
		producer.streamName = config.getStream();
		producer.config = config;
		producer.client = builder.build();
		return producer;
	}
	
	public String write(final String partitionKey, final ByteBuffer data) {
		final PutRecordResult result = client.putRecord(streamName, data, partitionKey);
		return result.getSequenceNumber();
	}
	
	public int writeBuffer(List<PutRecordsRequestEntry> entries) {
		
		try {
			PutRecordsRequest request = new PutRecordsRequest();
			if(entries.isEmpty()) {
				System.out.println("No records to write to stream " + streamName);
				return 0;
			}
			request.setStreamName(streamName);
			request.setRecords(entries);
			final PutRecordsResult result = client.putRecords(request);
			if(result.getFailedRecordCount() == 0) {
				System.out.println("Written " + entries.size() + " to stream " + streamName);
			} else {
				System.out.println("Written " + (entries.size() - result.getFailedRecordCount()) + " to stream " + streamName + " with " + result.getFailedRecordCount() + " errors");
			}
			return entries.size() - result.getFailedRecordCount();
		} catch(Exception e) {
			handleError(e);
			return 0;
		}
	}
	
	protected static void handleError(final Exception e) {
		final StringWriter sw = new StringWriter();
		final PrintWriter pw = new PrintWriter(sw);
		e.printStackTrace(pw);
		System.err.print(sw.getBuffer().toString());
	}
}
