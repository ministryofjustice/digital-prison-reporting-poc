package uk.gov.justice.dpr.kinesis;

import java.io.Serializable;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;

public class KinesisConfiguration implements Serializable {
	
	private static final long serialVersionUID = -5869654664577303034L;
	private String awsAccessKeyId;
	private String awsSecretKey;
	private String region;
	private String stream;
	
	public String getAwsAccessKeyId() {
		return awsAccessKeyId;
	}
	public void setAwsAccessKeyId(String awsAccessKeyId) {
		this.awsAccessKeyId = awsAccessKeyId;
	}
	public String getAwsSecretKey() {
		return awsSecretKey;
	}
	public void setAwsSecretKey(String awsSecretKey) {
		this.awsSecretKey = awsSecretKey;
	}
	public String getRegion() {
		return region;
	}
	public void setRegion(String region) {
		this.region = region;
	}
	public String getStream() {
		return stream;
	}
	public void setStream(String stream) {
		this.stream = stream;
	}
	
	public AWSCredentialsProvider getCredentialsProvider() {
		if(awsAccessKeyId == null || awsSecretKey == null) {
			return null;
		}
		return new AWSStaticCredentialsProvider( new BasicAWSCredentials(awsAccessKeyId, awsSecretKey));	
	}
}
