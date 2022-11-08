package uk.gov.justice.dpr.configuration;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.kinesis.KinesisSink;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.OutputMode;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;

import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConverters;
import uk.gov.justice.dpr.queue.MessageFileLoader;
import uk.gov.justice.dpr.queue.Queue;

public class BaseApplicationConfiguration {

	protected static AmazonSQS createSQS(final Map<String,String> params) {
		final String queueRegion = getRequiredParameter(params, "source.region");
		final String awsAccessKey = getOptionalParameter(params, "source.accessKey");
		final String awsSecretKey = getOptionalParameter(params, "source.secretKey");
		
		AWSCredentialsProvider provider = null;
		if(awsAccessKey != null && awsSecretKey != null) {
			provider = new AWSCredentialsProvider() {
				@Override
				public AWSCredentials getCredentials() {
					return new BasicAWSCredentials(awsAccessKey, awsSecretKey);
				}

				@Override
				public void refresh() {
				}
			};
		}
		
		AmazonSQSClientBuilder builder = AmazonSQSClient.builder(); 
		builder.setRegion(queueRegion);
		if(provider != null) {
			builder = builder.withCredentials(provider);
		}
		return builder.build();
	}
	
	protected static Queue getQueue(final SparkSession spark, AmazonSQS client, final Map<String,String> params) {
		final String queueName = getRequiredParameter(params, "source.queue");
		final String queueRegion = getRequiredParameter(params, "source.region");
		final String awsAccessKey = getOptionalParameter(params, "source.accessKey");
		final String awsSecretKey = getOptionalParameter(params, "source.secretKey");
		
		AWSCredentialsProvider provider = null;
		if(awsAccessKey != null && awsSecretKey != null) {
			provider = new AWSCredentialsProvider() {
				@Override
				public AWSCredentials getCredentials() {
					return new BasicAWSCredentials(awsAccessKey, awsSecretKey);
				}

				@Override
				public void refresh() {
				}
			};
		}
		
		if(client == null) {
			client = createSQS(params);
		}
		
		AmazonS3ClientBuilder s3Builder = AmazonS3Client.builder().withRegion(queueRegion);
		if(provider != null) {
			s3Builder = s3Builder.withCredentials(provider);
		}
		
	
		final AmazonS3 s3client = s3Builder.build();
		final MessageFileLoader loader = new MessageFileLoader(s3client);
		final Queue queue = new Queue(client, loader, queueName);
		
		return queue;
	}
	
	

	// https://stackoverflow.com/questions/72882055/spark-structured-streaming-with-kinesis-on-localstack-error-while-fetching-shar
	// https://github.com/qubole/kinesis-sql
	protected static DataStreamReader getKinesisDataStreamReader(final SparkSession spark, final Map<String,String> params) {
		final String streamName = getRequiredParameter(params, "source.stream");
		final String endpointUrl = getRequiredParameter(params, "source.url");
		final String awsAccessKey = getOptionalParameter(params, "source.accessKey");
		final String awsSecretKey = getOptionalParameter(params, "source.secretKey");
		
		final DataStreamReader dsr = spark.readStream()   // readstream() returns type DataStreamReader
			      .format("kinesis")
			      .option("streamName", streamName)
			      .option("endpointUrl", endpointUrl)
			      // .option("checkpointInterval", <same as trigger>)
			      // .option("checkpointLocation", "/tmp")
			      // shard management
			      // .option("initialPosition", "trim_horizon")
			      .option("startingposition", "TRIM_HORIZON")
			      //.option("maxFetchRate", "1.5")
			      //.option("minFetchPeriod", "15s")
			      //.option("maxFetchDuration", "20s")
			      //.option("shardFetchInterval", "10m")
			      //.option("fetchBufferSize", "1gb")
		        
			      .option("kinesis.client.avoidEmptyBatches", "true")
			      // schema and data format
			      .option("inferSchema", "true")
			      .option("classification", "json");
		
		if(awsAccessKey != null && !awsAccessKey.isEmpty() && awsSecretKey != null && !awsSecretKey.isEmpty() ) {
		      dsr.option("awsAccessKeyId", awsAccessKey)
		         .option("awsSecretKey", awsSecretKey);
		} else {
			dsr.option("awsUseInstanceProfile", false);
		}
		
		return dsr;
	}
	

	
	protected static KinesisSink getKinesisSink(final SparkSession spark, final Map<String,String> params) {
		final String sinkUrl = getRequiredParameter(params, "sink.url");
		final String sinkStream = getRequiredParameter(params, "sink.stream");
		final String awsAccessKey = getOptionalParameter(params, "sink.accessKey");
		final String awsSecretKey = getOptionalParameter(params, "sink.secretKey");
		
		// create params
		Map<String, String> kinesisParams = new HashMap<String,String>();
		
		// note that this doesn't work if you use streamName and endpointUrl etc (camel-case).
		// it only works if you use lowercase only
		kinesisParams.put("streamname", sinkStream);
		kinesisParams.put("endpointurl", sinkUrl);
		if(awsAccessKey != null && !awsAccessKey.isEmpty())
			kinesisParams.put("awsaccesskeyid", awsAccessKey);
		if(awsSecretKey != null && !awsSecretKey.isEmpty())
			kinesisParams.put("awssecretkey", awsSecretKey);
		
	
		@SuppressWarnings("deprecation")
		scala.collection.immutable.Map<String, String> kp = JavaConverters.mapAsScalaMapConverter(kinesisParams).
				asScala()
				.toMap(Predef.<Tuple2<String, String>>conforms());
		
		return new KinesisSink(spark.sqlContext(), kp, OutputMode.Update());
	}
	
	protected static String getRequiredParameter(final Map<String, String> params, final String name) {
		final String value = params.getOrDefault(name, null);
		if(value == null || value.isEmpty()) 
			throw new IllegalArgumentException(name + " is a required parameter and is missing");
		
		return value;
	}
	
	protected static String getOptionalParameter(final Map<String, String> params, final String name) {
		return params.getOrDefault(name, null);
	}
}
