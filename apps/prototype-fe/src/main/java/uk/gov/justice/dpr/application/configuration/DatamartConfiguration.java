package uk.gov.justice.dpr.application.configuration;

import javax.annotation.Resource;
import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.annotation.Lazy;

@Configuration
@ImportResource({"classpath:spring/properties-configuration.xml", "classpath:spring/jdbc/datamart-source-configuration.xml"}) 
public class DatamartConfiguration {

	@Value("${governance.hub.url:}")
	public String url;
	
	@Value("${governance.hub.user:}")
	public String user;
	
	@Value("${governance.hub.server:}")
	public String serverName;
	
	@Value("${governance.hub.password:}")
	public String password;
	
	
	public @Resource(name="datamart-datasource") @Lazy DataSource datasource;
	
	@Bean(name="datamart-source")
	@Lazy
	public DataSource getDatamartSource() {
		return datasource;
	}
	
}