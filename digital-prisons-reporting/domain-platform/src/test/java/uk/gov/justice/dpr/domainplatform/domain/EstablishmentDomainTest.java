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
public class EstablishmentDomainTest extends BaseDomainTest {

	// =============================================================================================
	// Our interest is in the transform, mappings and violations CONTENT - not the
	// location on disk
	// nor the operation of the domain executor
	// =============================================================================================

	// DPR-132 : [https://dsdmoj.atlassian.net/browse/DPR-132] agency_location data from
	// NOMIS
	@Test
	public void shouldLoadDomainDefinition() throws Exception {
		final DomainDefinition def = this.loadAndValidateDomain("/domains/establishment.domain.json");
		assertNotNull(def);
	}

	// DPR-132 [https://dsdmoj.atlassian.net/browse/DPR-132] establishment data
	@Test
	public void shouldExecuteTransformOnEstablishmentTable() throws Exception {
		final DomainDefinition def = this.loadAndValidateDomain("/domains/establishment.domain.json");
		final TableDefinition table = this.getTableByName(def, "establishment");

		assertNotNull(table);
		// setup a source
		Dataset<Row> source = loadParquetDataframe("/sample/sample-nomis.agency_locations.parquet",
				"nomis.agency_locations.parquet");

		Dataset<Row> result = this.applyTransform(table, source);

		assertNotNull(result);
		assertFalse(result.isEmpty());
		assertEquals(source.count(), result.count());

		result.show(false);

		// check columns
		assertTrue(hasColumn(result, "id"));
		assertTrue(hasColumn(result, "name"));
		assertEquals(2, columnCount(result));
		// check results
		assertEquals("string", getType(result, "id"));
		assertEquals("string", getType(result, "name"));

	}

	// DPR-132 [https://dsdmoj.atlassian.net/browse/DPR-132] Living Unit data
	@Test
	public void shouldExecuteTransformOnLivingUnitTable() throws Exception {
		final DomainDefinition def = this.loadAndValidateDomain("/domains/establishment.domain.json");
		final TableDefinition table = this.getTableByName(def, "living_unit");

		assertNotNull(table);
		Dataset<Row> source = loadParquetDataframe("/sample/sample-nomis.agency_internal_locations.parquet",
				"nomis.agency_internal_locations.parquet");

		Dataset<Row> result = this.applyTransform(table, source);

		assertNotNull(result);
		// it may be empty assertFalse(result.isEmpty());

		result.show(false);

		// check columns
		assertTrue(hasColumn(result, "id"));
		assertTrue(hasColumn(result, "code"));
		assertTrue(hasColumn(result, "establishment_id"));
		assertTrue(hasColumn(result, "name"));
		assertEquals(4, columnCount(result));
		// check results
	}
}
