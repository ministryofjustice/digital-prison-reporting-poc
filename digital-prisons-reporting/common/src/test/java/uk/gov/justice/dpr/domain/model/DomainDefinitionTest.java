package uk.gov.justice.dpr.domain.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.fasterxml.jackson.databind.ObjectMapper;

@RunWith(MockitoJUnitRunner.class)
public class DomainDefinitionTest {

	@Test
	public void shouldLoadADomainDefinitionFromJSONIntoDomainDefinition() throws IOException {
		
		final String json = getResource("/domain/simple-domain-one-table.json");
		final DomainDefinition domain = new ObjectMapper().readValue(json, DomainDefinition.class);
		
		assertNotNull(domain);
		assertEquals("example", domain.getName());
	}
	
	protected String getResource(final String resource) throws IOException {
		final InputStream stream = getStream(resource);
		return IOUtils.toString(stream);
	}
	
	public static InputStream getStream(final String resource) {
		InputStream stream = System.class.getResourceAsStream(resource);
		if(stream == null) {
			stream = System.class.getResourceAsStream("/src/test/resources" + resource);
			if(stream == null) {
				stream = System.class.getResourceAsStream("/target/test-classes" + resource);
				if(stream == null) {
					Path root = Paths.get(".").normalize().toAbsolutePath();
					stream = System.class.getResourceAsStream(root.toString() + "/src/test/resources" + resource);
					if(stream == null) {
						stream = DomainDefinitionTest.class.getResourceAsStream(resource);
					}
				}
			}
		}
		return stream;
	}
}
