package uk.gov.justice.dpr.domainplatform.configuration;

import java.util.Map;

import org.apache.spark.sql.SparkSession;

import uk.gov.justice.dpr.domain.DomainRepository;
import uk.gov.justice.dpr.configuration.BaseApplicationConfiguration;
import uk.gov.justice.dpr.domainplatform.job.TableChangeMonitor;
import uk.gov.justice.dpr.queue.Queue;

public class DomainPlatform extends BaseApplicationConfiguration {

	public static TableChangeMonitor initialise(final SparkSession spark, final Map<String,String> params) {
		if(params == null || params.isEmpty()) {
			throw new IllegalArgumentException("No Parameters provided");
		}
		
		if(spark == null) {
			throw new IllegalArgumentException("Spark Session is null");
		}

		final String domainFilesPath = getRequiredParameter(params, "domain.files.path");
		final String domainRepoPath = getRequiredParameter(params, "domain.repo.path");
		final String sourcePath = getRequiredParameter(params, "cloud.platform.path");
		final String targetPath = getRequiredParameter(params, "target.path");
		
		getOrCreateDomainRepository(spark, domainFilesPath, domainRepoPath);

		final Queue queue = getQueue(spark, null, params);
		return new TableChangeMonitor(spark, queue, domainRepoPath, sourcePath, targetPath);
	}
	
	protected static void getOrCreateDomainRepository(final SparkSession spark, final String domainFilesPath, final String domainRepositoryPath) {
		final DomainRepository repository = new DomainRepository(spark, domainFilesPath, domainRepositoryPath);
		if(!repository.exists()) {
			System.out.println("Domain repository cache missing. Caching domains...");
			repository.touch();
			System.out.println("Domain repository cached.");
		}
	}

}
