package uk.gov.justice.dpr.domainplatform.domain;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

/**
 * This set of tests ensures that the domains are correctly configured
 * And would run in an integrated environment
 * 
 * This creates the various tables in the prisons fabric
 * Then runs the actual domains against them
 * And checks that the conversions/transformations have occurred
 * 
 * DPR-128, DPR-129, DPR-130, DPR-131
 * 
 * @author dominic.messenger
 *
 */
@RunWith(BlockJUnit4ClassRunner.class)
public class RealDomainTests {

	@Test
	public void shouldExecuteUseOfForceIncidentReport_DPR128() {
		
	}
}
