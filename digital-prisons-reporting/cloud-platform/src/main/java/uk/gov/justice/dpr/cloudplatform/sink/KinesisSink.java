package uk.gov.justice.dpr.cloudplatform.sink;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;

// see  https://docs.databricks.com/_static/notebooks/structured-streaming-kinesis-sink.html

public class KinesisSink extends ForeachWriter<byte[]> {
	
	private static final long serialVersionUID = 1540646379127480291L;
	
	protected AmazonKinesis client;
	protected AmazonKinesisClientBuilder builder;
	
	private String stream;
	private String region;
	private String awsAccessKey = null;
	private String awsSecretKey = null;

	public KinesisSink(final String region, final String stream) {
		this.stream = stream;
		this.region = region;
		
	}

	public KinesisSink(final String region, final String stream, final String awsAccessKey, final String awsSecretKey) {
		this.stream = stream;
		this.region = region;
		this.awsAccessKey = awsAccessKey;
		this.awsSecretKey = awsSecretKey;
	}
	
	public void write(final Dataset<Row> batch) {
		try {
			System.out.println("KinesisSink::write");
			
			final List<Row> json = jsonify(batch).collectAsList();
			for(final Row r : json) {
				process(r.getString(0).getBytes());
			}
		} catch(Exception e) {
			System.err.println(e.getMessage());
			throw e;
		}
	}
	
	protected Dataset<Row> jsonify(final Dataset<Row> df) {
		return df
				.select(functions.struct("*").as("s"))
				.select(functions.to_json(functions.col("s")).as("_json"))
				.select(functions.col("_json"));
	}
	
	@Override
	public void close(Throwable errorOrNull) {
		client.shutdown();
		client = null;
	}

	@Override
	public boolean open(long partitionId, long epochId) {
		client = createClient();
		System.out.println("KinesisSink::open(" + partitionId + "," + epochId + ")");
		return client != null;
	}

	@Override
	public void process(byte[] value) {

		final PutRecordRequest request = new PutRecordRequest()
			.withStreamName(stream)
			.withData(ByteBuffer.wrap(value));
		
		@SuppressWarnings("unused")
		final PutRecordResult prr = client.putRecord(request);
			
	}
	
	@SuppressWarnings("static-access")
	protected AmazonKinesis createClient() {
		if(StringUtils.isEmpty(awsAccessKey) || StringUtils.isEmpty(awsSecretKey)) {
			return builder.standard().build();
		} else {
			return builder.standard()
	        .withRegion(region)
	        .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(awsAccessKey, awsSecretKey)))
	        .build();
		}
	}
	 

}
