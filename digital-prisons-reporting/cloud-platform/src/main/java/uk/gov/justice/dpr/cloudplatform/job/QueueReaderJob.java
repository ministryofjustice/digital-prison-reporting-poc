package uk.gov.justice.dpr.cloudplatform.job;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamWriter;

import uk.gov.justice.dpr.cloudplatform.zone.CuratedZone;
import uk.gov.justice.dpr.cloudplatform.zone.RawZone;
import uk.gov.justice.dpr.cloudplatform.zone.StructuredZone;
import uk.gov.justice.dpr.kinesis.KinesisWriter;
import uk.gov.justice.dpr.queue.Queue;

public class QueueReaderJob extends BaseReportingHubJob {

	private final SparkSession spark;
	private final Queue queue;
	
	public QueueReaderJob(final SparkSession spark, final Queue queue, RawZone raw, StructuredZone structured, CuratedZone curated, KinesisWriter sink) {
		super(raw, structured, curated, sink);
		this.spark = spark;
		this.queue = queue;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public DataStreamWriter run() {
		Dataset<Row> df = queue.getQueuedMessages(spark);
		if(df != null) {
			try {
				new BaseReportingHubJob.Function().call(df, Long.valueOf(0));
			} catch (Exception e) {
				handleError(e);
			}
		}
		return null;
	}

}
