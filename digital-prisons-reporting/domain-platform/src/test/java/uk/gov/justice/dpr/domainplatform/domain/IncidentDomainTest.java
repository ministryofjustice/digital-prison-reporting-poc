package uk.gov.justice.dpr.domainplatform.domain;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

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
		assertTrue(hasColumn(result, "booking_id"));
		assertEquals(6, columnCount(result));
		// check results
	}
	
	// DPR-128 [https://dsdmoj.atlassian.net/browse/DPR-128] incident data
	@Test
	public void shouldExecuteMappingOnIncidentTable() throws Exception {
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
		
		result = this.applyMapping(table, result);
		
		// check columns
		assertTrue(hasColumn(result, "id"));
		assertTrue(hasColumn(result, "type"));
		assertTrue(hasColumn(result, "incident_date"));
		assertTrue(hasColumn(result, "agency_id"));
		assertTrue(hasColumn(result, "offender_no"));
		assertTrue(hasColumn(result, "booking_id"));
		assertEquals(6, columnCount(result));
		// check results
		assertEquals("integer", getType(result, "id"));
		assertEquals("string", getType(result, "type"));
		assertEquals("timestamp", getType(result, "incident_date"));
		assertEquals("string", getType(result, "agency_id"));
		assertEquals("string", getType(result, "offender_no"));
		assertEquals("integer", getType(result, "booking_id"));
		
		// save to disk and re-read
		this.saveToDisk("incident", "incident", result);
		
		Dataset<Row> df_saved = this.readFromDisk("incident", "incident");
		
		assertEquals(6, columnCount(df_saved));
		// check results
		assertEquals("integer", getType(df_saved, "id"));
		assertEquals("string", getType(df_saved, "type"));
		assertEquals("timestamp", getType(df_saved, "incident_date"));
		assertEquals("string", getType(df_saved, "agency_id"));
		assertEquals("string", getType(df_saved, "offender_no"));
		assertEquals("integer", getType(df_saved, "booking_id"));
		
		this.mergeToDisk("incidental", "incident", "id", result);
		Dataset<Row> df_merged = this.readFromDisk("incidental", "incident");
				
		assertEquals(6, columnCount(df_merged));
		// check results
		assertEquals("integer", getType(df_merged, "id"));
		assertEquals("string", getType(df_merged, "type"));
		assertEquals("timestamp", getType(df_merged, "incident_date"));
		assertEquals("string", getType(df_merged, "agency_id"));
		assertEquals("string", getType(df_merged, "offender_no"));
		assertEquals("integer", getType(df_merged, "booking_id"));
		
	}
	
	
	// DPR-129 [https://dsdmoj.atlassian.net/browse/DPR-129] demographics data
	@Test
	public void shouldExecuteTransformOnDemographicsTable() throws Exception {
		final DomainDefinition def = this.loadAndValidateDomain("/domains/incident.domain.json");
		final TableDefinition table = this.getTableByName(def, "demographics");

		assertNotNull(table);
		// setup a source or 2
		Dataset<Row> df_offenders = loadParquetDataframe("/sample/offenders.parquet", "offenders.parquet");
		Dataset<Row> df_offender_bookings = loadParquetDataframe("/sample/offender-bookings.parquet", "offender-bookings.parquet");

		Map<String, Dataset<Row>> refs = new HashMap<String, Dataset<Row>>();
		
		refs.put("nomis.offenders", df_offenders);
		refs.put("nomis.offender_bookings", df_offender_bookings);
		
		Dataset<Row> result = this.applyTransform(refs, table.getTransform());

		assertNotNull(result);
		// it may be empty assertFalse(result.isEmpty());

		result.show(false);

		// check columns
		assertTrue(hasColumn(result, "id"));
		assertTrue(hasColumn(result, "birth_date"));
		assertTrue(hasColumn(result, "living_unit_id"));
		assertTrue(hasColumn(result, "first_name"));
		assertTrue(hasColumn(result, "last_name"));
		assertTrue(hasColumn(result, "offender_no"));
		assertEquals(6, columnCount(result));
		// check results
	}
}
