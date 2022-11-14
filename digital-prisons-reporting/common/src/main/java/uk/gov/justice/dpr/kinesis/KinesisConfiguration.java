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
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((awsAccessKeyId == null) ? 0 : awsAccessKeyId.hashCode());
		result = prime * result + ((awsSecretKey == null) ? 0 : awsSecretKey.hashCode());
		result = prime * result + ((region == null) ? 0 : region.hashCode());
		result = prime * result + ((stream == null) ? 0 : stream.hashCode());
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		KinesisConfiguration other = (KinesisConfiguration) obj;
		if (awsAccessKeyId == null) {
			if (other.awsAccessKeyId != null)
				return false;
		} else if (!awsAccessKeyId.equals(other.awsAccessKeyId))
			return false;
		if (awsSecretKey == null) {
			if (other.awsSecretKey != null)
				return false;
		} else if (!awsSecretKey.equals(other.awsSecretKey))
			return false;
		if (region == null) {
			if (other.region != null)
				return false;
		} else if (!region.equals(other.region))
			return false;
		if (stream == null) {
			if (other.stream != null)
				return false;
		} else if (!stream.equals(other.stream))
			return false;
		return true;
	}
	
	
}
