package uk.gov.justice.dpr.cdc;

public class KinesisEvent {
	private String data;
	private String streamName;
	private String partitionKey;
	private String sequenceNumber;
	private long approximateArrivalTimestamp;
	
	public String getData() {
		return data;
	}
	public void setData(String data) {
		this.data = data;
	}
	public String getStreamName() {
		return streamName;
	}
	public void setStreamName(String streamName) {
		this.streamName = streamName;
	}
	public String getPartitionKey() {
		return partitionKey;
	}
	public void setPartitionKey(String partitionKey) {
		this.partitionKey = partitionKey;
	}
	public String getSequenceNumber() {
		return sequenceNumber;
	}
	public void setSequenceNumber(String sequenceNumber) {
		this.sequenceNumber = sequenceNumber;
	}
	public long getApproximateArrivalTimestamp() {
		return approximateArrivalTimestamp;
	}
	public void setApproximateArrivalTimestamp(long approximateArrivalTimestamp) {
		this.approximateArrivalTimestamp = approximateArrivalTimestamp;
	}
	
	
}
