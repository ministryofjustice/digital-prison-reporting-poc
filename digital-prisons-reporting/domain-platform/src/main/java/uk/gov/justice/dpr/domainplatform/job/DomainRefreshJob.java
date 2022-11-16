package uk.gov.justice.dpr.domainplatform.job;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Set;

import org.apache.spark.sql.SparkSession;

import uk.gov.justice.dpr.domain.DomainRepository;
import uk.gov.justice.dpr.domain.model.DomainDefinition;
import uk.gov.justice.dpr.domainplatform.domain.DomainExecutor;

// DPR-137
// To take a domain and to do a full refresh
public class DomainRefreshJob {

	protected SparkSession spark;
	protected String domainFilesPath;
	protected String domainRepositoryPath;
	protected String sourcePath;
	protected String targetPath;
	
	protected DomainRepository repo;
	
	public DomainRefreshJob(final SparkSession spark, final String domainFilesPath, final String domainRepositoryPath, final String sourcePath, final String targetPath) {
		this.domainFilesPath = domainFilesPath;
		this.domainRepositoryPath = domainRepositoryPath;
		this.sourcePath = sourcePath;
		this.targetPath = targetPath;
		this.spark = spark;
		this.repo = new DomainRepository(spark, domainFilesPath, domainRepositoryPath);
	}
	
	public void run(final String name) {
		Set<DomainDefinition> domains = getDomains(name);
		System.out.println("Located " + domains.size() + " domains for name '" + name + "'");
		for(final DomainDefinition domain : domains) {
			processDomain(domain);
		}
	}
	
	protected Set<DomainDefinition> getDomains(final String name) {
		return this.repo.getForName(name);
	}
	
	protected void processDomain(final DomainDefinition domain) {
		try {
			System.out.println("DomainRefresh::process('" + domain.getName() + "') started");
			final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domain);
			executor.doFull();
			System.out.println("DomainRefresh::process('" + domain.getName() + "') completed");
		} catch(Exception e) {
			System.out.println("DomainRefresh::process('" + domain.getName() + "') failed");
			handleError(e);
		}
	}
	
	protected void handleError(final Exception e) {
		final StringWriter sw = new StringWriter();
		final PrintWriter pw = new PrintWriter(sw);
		e.printStackTrace(pw);
		System.err.print(sw.getBuffer().toString());
	}
}
