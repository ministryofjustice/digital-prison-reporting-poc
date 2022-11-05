package uk.gov.justice.dpr.cloudplatform.job;

import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.kinesis.KinesisSink;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.DataStreamWriter;

import uk.gov.justice.dpr.cdc.EventConverter;
import uk.gov.justice.dpr.cloudplatform.zone.CuratedZone;
import uk.gov.justice.dpr.cloudplatform.zone.RawZone;
import uk.gov.justice.dpr.cloudplatform.zone.StructuredZone;

public abstract class BaseReportingHubJob {

	protected RawZone raw = null;
	protected StructuredZone structured = null;
	protected CuratedZone curated = null;
	protected KinesisSink stream = null;
	
	
	public BaseReportingHubJob(final RawZone raw, final StructuredZone structured, final CuratedZone curated, final KinesisSink sink) {
		this.raw = raw;
		this.structured = structured;
		this.curated = curated;
		this.stream = sink;
	}
	
	@SuppressWarnings("rawtypes")
	public abstract DataStreamWriter run();
	
	@SuppressWarnings("rawtypes")
	public DataStreamWriter run(final DataStreamReader in) {
		return run(in.load());
	}
	
	@SuppressWarnings("rawtypes")
	public DataStreamWriter run(final Dataset<Row> df) {
		return df.writeStream().foreachBatch(new BaseReportingHubJob.Function());
	}
	
	public RawZone getRawZone() {
		return raw;
	}
	
	public StructuredZone getStructuredZone() {
		return structured;
	}
	
	public CuratedZone getCuratedZone() {
		return curated;
	}
	
	public KinesisSink getOutStream() {
		return stream;
	}
	
	public class Function implements VoidFunction2<Dataset<Row>, Long> {
		private static final long serialVersionUID = 8028572153253155936L;

		@Override
		public void call(Dataset<Row> df, Long batchId) throws Exception {
			System.out.println("Running batch " + batchId );
			if(!df.isEmpty()) {

				try {
					// raw
					if(raw != null) raw.writeBatch(df, batchId);
					
					Dataset<Row> internalEventDF = EventConverter.fromKinesis(df);
					
					// structured
					if(structured != null) {
						structured.writeBatch(internalEventDF, batchId);
					}
	
					// curated
					if(curated != null) {
						curated.writeBatch(internalEventDF, batchId);
					}
					
					// pass onto domain
					// use org.apache.spark.sql.kinesis.KinesisSink
					// call addBatch(batchId, internalEventDF)
					// MUST HAVE A FIELD data and an optional PARTITIONKEY
					Dataset<Row> out = EventConverter.toKinesis(internalEventDF);
					
					stream.addBatch(batchId, out);
					
				} catch(Exception e) {
					System.err.println(e.getMessage());
				}
			}
		}
		
	}
}
