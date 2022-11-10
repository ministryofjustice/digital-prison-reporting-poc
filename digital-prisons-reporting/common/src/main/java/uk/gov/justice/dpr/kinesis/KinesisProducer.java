package uk.gov.justice.dpr.kinesis;

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

	private static Map<KinesisConfiguration, KinesisProducer> CACHE = new HashMap<KinesisConfiguration, KinesisProducer>();
	
	private AmazonKinesis client;
	private KinesisConfiguration config;
	private String streamName;
	
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
	
	public int writeBuffer(Map<String, ByteBuffer> records) {
		
		PutRecordsRequest request = new PutRecordsRequest();
		List<PutRecordsRequestEntry> entries = new ArrayList<PutRecordsRequestEntry>();
		for(final String key : records.keySet()) {
			entries.add(new PutRecordsRequestEntry().withData(records.get(key)).withPartitionKey(key));
		}
		if(entries.isEmpty()) {
			System.out.println("No records to write to stream " + streamName);
			return 0;
		}
		request.setStreamName(streamName);
		request.setRecords(entries);
		final PutRecordsResult result = client.putRecords(request);
		if(result.getFailedRecordCount() == 0) {
			System.out.println("Written " + records.size() + " to stream " + streamName);
		} else {
			System.out.println("Written " + (records.size() - result.getFailedRecordCount()) + " to stream " + streamName + " with " + result.getFailedRecordCount() + " errors");
		}
		return records.size() - result.getFailedRecordCount();
	}
	
}
