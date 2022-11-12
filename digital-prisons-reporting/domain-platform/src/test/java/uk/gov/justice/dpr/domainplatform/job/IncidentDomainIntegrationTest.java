package uk.gov.justice.dpr.domainplatform.job;

import java.io.IOException;
import java.util.UUID;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import uk.gov.justice.dpr.BaseSparkTest;
import uk.gov.justice.dpr.delta.DeltaLakeService;
import uk.gov.justice.dpr.domain.model.DomainDefinition;
import uk.gov.justice.dpr.domain.model.TableDefinition;
import uk.gov.justice.dpr.domainplatform.domain.BaseDomainTest;

/**
 * This will do 2 things : 
 *    - load a curated table and do a full refresh
 *    - take a cdc stream and do an incremental refresh
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class IncidentDomainIntegrationTest extends BaseDomainTest {

	protected String sourcePath;
	protected String targetPath;
	
	@Before
	public void before() {
		super.before();
		sourcePath = folder.getRoot().getAbsolutePath() + "/source";
		targetPath = folder.getRoot().getAbsolutePath() + "/target";
	}
	
	
	@Test
	@Ignore
	public void shouldTransformFullRefreshTableCorrectly_Incident() throws Exception {
		loadCuratedTable(sourcePath, "prisons", "report", "/sample/tables/use-of-force.report.parquet");
		final TableDefinition incident = loadTableDefinition("/domains/incident.domain.json", "incident");
		
	}
	
	protected TableDefinition loadTableDefinition(final String resource, final String table) throws Exception {
		final DomainDefinition def = loadAndValidateDomain(resource);
		return getTableByName(def, table);
		
	}
	
	protected void loadCuratedTable(final String sourcePath, final String namespace, final String table, final String source) throws IOException {
		Dataset<Row> df = this.loadParquetDataframe(source, "table" + UUID.randomUUID() + ".parquet");
		loadCuratedTable(sourcePath, namespace, table, df);
	}
	
	protected void loadCuratedTable(final String sourcePath, final String namespace, final String table, final Dataset<Row> source) {
		final DeltaLakeService service = new DeltaLakeService();
		service.replace(sourcePath, namespace, table, source);
	}
}
