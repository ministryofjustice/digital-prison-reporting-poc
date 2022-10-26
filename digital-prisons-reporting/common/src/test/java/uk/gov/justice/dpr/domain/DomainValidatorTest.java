package uk.gov.justice.dpr.domain;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.fasterxml.jackson.databind.ObjectMapper;

import uk.gov.justice.dpr.ResourceLoader;
import uk.gov.justice.dpr.domain.model.DomainDefinition;

@RunWith(MockitoJUnitRunner.class)
public class DomainValidatorTest {

	@Test
	public void shouldValidateDomain() throws IOException {
		
		final String json = ResourceLoader.getResource(DomainValidatorTest.class, "/domain/simple-domain-one-table.json");
		final DomainDefinition domain = new ObjectMapper().readValue(json, DomainDefinition.class);
	
		assertNotNull(domain);
		
		final DomainValidator loader = new DomainValidator();
		loader.validate(domain);
	}
}
