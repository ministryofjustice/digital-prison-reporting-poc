package uk.gov.justice.dpr.domain;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.fasterxml.jackson.databind.ObjectMapper;

import uk.gov.justice.dpr.domain.model.DomainDefinition;

@RunWith(MockitoJUnitRunner.class)
public class DomainLoaderTest {

	@Test
	public void shouldValidateDomain() throws IOException {
		
		final String json = getResource("/domain/simple-domain-one-table.json");
		final DomainDefinition domain = new ObjectMapper().readValue(json, DomainDefinition.class);
	
		assertNotNull(domain);
		
		final DomainLoader loader = new DomainLoader();
		loader.validate(domain);
	}
	
	
	protected String getResource(final String resource) throws IOException {
		final InputStream stream = System.class.getResourceAsStream(resource);
		return IOUtils.toString(stream);
	}
}
