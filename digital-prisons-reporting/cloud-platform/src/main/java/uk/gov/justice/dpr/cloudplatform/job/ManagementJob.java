package uk.gov.justice.dpr.cloudplatform.job;

import java.util.Set;

import org.apache.spark.sql.SparkSession;

import uk.gov.justice.dpr.delta.DeltaLakeService;
import uk.gov.justice.dpr.service.SourceReferenceService;
import uk.gov.justice.dpr.service.SourceReferenceService.SourceReference;

public class ManagementJob {
	private final SparkSession spark;
	private final String zone;
	
	public ManagementJob(final SparkSession spark, final String zone) {
		this.spark = spark;
		this.zone = zone;
	}
	
	public void compactAll() {
		DeltaLakeService delta = new DeltaLakeService();
		Set<SourceReference> references = SourceReferenceService.getReferences();
		
		for(SourceReference reference : references) {
			delta.compact(spark, zone, reference.getSource(), reference.getTable(), 16);
		}
	}
}
