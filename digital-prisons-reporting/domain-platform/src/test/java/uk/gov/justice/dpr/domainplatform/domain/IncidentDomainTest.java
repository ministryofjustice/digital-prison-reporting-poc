package uk.gov.justice.dpr.domainplatform.domain;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import uk.gov.justice.dpr.domain.model.DomainDefinition;
import uk.gov.justice.dpr.domain.model.TableDefinition;

@RunWith(BlockJUnit4ClassRunner.class)
public class IncidentDomainTest extends BaseDomainTest {
	
	//=============================================================================================
	// Our interest is in the transform, mappings and violations CONTENT - not the location on disk
	// nor the operation of the domain executor
	//=============================================================================================
	
	
	// DPR-128 : [https://dsdmoj.atlassian.net/browse/DPR-128] incident data from use-of-force
	@Test
	public void shouldLoadDomainDefinition() throws Exception {
		final DomainDefinition def = this.loadAndValidateDomain("/domains/incident.domain.json");
		assertNotNull(def);
	}
	
	// DPR-128 [https://dsdmoj.atlassian.net/browse/DPR-128] incident data
	@Test
	public void shouldExecuteTransformOnIncidentTable() throws Exception {
		final DomainDefinition def = this.loadAndValidateDomain("/domains/incident.domain.json");
		final TableDefinition table = this.getTableByName(def, "incident");
		
		assertNotNull(table);
		// setup a source or 2
		Dataset<Row> source = loadParquetDataframe("/sample/use-of-force.report.parquet", "use-of-force.report.parquet");
		
		Dataset<Row> result = this.applyTransform(table, source);
		
		assertNotNull(result);
		assertFalse(result.isEmpty());
		assertEquals(source.count(), result.count());
		
		result.show(false);
		
		// check columns
		assertTrue(hasColumn(result, "id"));
		assertTrue(hasColumn(result, "type"));
		assertTrue(hasColumn(result, "incident_date"));
		assertTrue(hasColumn(result, "agency_id"));
		assertTrue(hasColumn(result, "offender_no"));
		assertEquals(5, columnCount(result));
		// check results
	}
	
	
}
