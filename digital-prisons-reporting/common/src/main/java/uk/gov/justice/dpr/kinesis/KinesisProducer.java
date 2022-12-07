package uk.gov.justice.dpr.kinesis;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
import com.amazonaws.services.kinesis.model.Record;

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
	
	public int writeBuffer(List<PutRecordsRequestEntry> entries, int depth) {
		
		try {
			if(depth <= 3) {
				PutRecordsRequest request = new PutRecordsRequest();
				if(entries.isEmpty()) {
					System.out.println("No records to write to stream " + streamName);
					return 0;
				}
				request.setStreamName(streamName);
				request.setRecords(entries);
				final PutRecordsResult result = client.putRecords(request);
				int retrySize = 0;
				if(result.getFailedRecordCount() == 0) {
					System.out.println("Written " + entries.size() + " to stream " + streamName);
				} else {
					// we need to extract the errors and retry them after a timeout
					List<PutRecordsRequestEntry> retry = new ArrayList<PutRecordsRequestEntry>();
					
					System.out.println("Written " + (entries.size() - result.getFailedRecordCount()) + " to stream " + streamName + " with " + result.getFailedRecordCount() + " errors [Retry " + depth + "]");
					List<PutRecordsResultEntry> resultEntries = result.getRecords();
		            int  i = 0;
		            for (PutRecordsResultEntry resultEntry : resultEntries) {
		                final String errorCode = resultEntry.getErrorCode();
		                if (null != errorCode) {
		                    switch (errorCode) {
		                    case "ProvisionedThroughputExceededException":
		                        retry.add(entries.get(i));
		                        break;
		                    case "InternalFailure":
		                        // Records are processed in the order you submit them,
		                        // so this will align with the initial record batch
		                        handleFailedRecord(entries.get(i), i, errorCode + ":" + resultEntry.getErrorMessage());
		                        break;
		                    default:
		                        validateSuccessfulRecord(entries.get(i), i, resultEntry);
		                        break;
		                    }
		                } else {
		                    validateSuccessfulRecord(entries.get(i), i, resultEntry);
		                }
		                ++i;
		            }
		            TimeUnit.SECONDS.sleep(1);
		            retrySize = writeBuffer(retry, depth++);
				} 
				return entries.size() + retrySize - result.getFailedRecordCount();
			} else {
				int i = 0;
				for(final PutRecordsRequestEntry e : entries) {
					handleFailedRecord(e, i, "ProvisionedThroughputExceededException: Rate exceeded");
					i++;
				}
			}
			return 0;
		} catch(Exception e) {
			handleError(e);
			return 0;
		}
	}
	
	private void validateSuccessfulRecord(PutRecordsRequestEntry record, final int position, PutRecordsResultEntry resultEntry)  {
        if (null == resultEntry.getSequenceNumber() || null == resultEntry.getShardId()
                || resultEntry.getSequenceNumber().isEmpty() || resultEntry.getShardId().isEmpty()) {
            // Some kind of other error, handle it.
            handleFailedRecord(record, position, "Missing SequenceId or ShardId.");
        }
    }
	
	private void handleFailedRecord(PutRecordsRequestEntry record, final int position, final String cause) {
		System.err.println("Error on Record "  + position + " " + record.getPartitionKey() + ": " + cause );
    }
	
	protected static void handleError(final Exception e) {
		final StringWriter sw = new StringWriter();
		final PrintWriter pw = new PrintWriter(sw);
		e.printStackTrace(pw);
		System.err.print(sw.getBuffer().toString());
	}
}
