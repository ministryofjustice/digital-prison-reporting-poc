package uk.gov.justice.dpr.cloudplatform.job;

import org.apache.spark.sql.kinesis.KinesisSink;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.DataStreamWriter;

import uk.gov.justice.dpr.cloudplatform.zone.CuratedZone;
import uk.gov.justice.dpr.cloudplatform.zone.RawZone;
import uk.gov.justice.dpr.cloudplatform.zone.StructuredZone;

public class StreamReaderJob extends BaseReportingHubJob {

	private final DataStreamReader dsr;
	
	public StreamReaderJob(DataStreamReader dsr, RawZone raw, StructuredZone structured, CuratedZone curated, KinesisSink sink) {
		super(raw, structured, curated, sink);
		this.dsr = dsr;
	}


	@SuppressWarnings("rawtypes")
	@Override
	public DataStreamWriter run() {
		return run(dsr);
	}
}
