package uk.gov.justice.dpr.application.configuration;


import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.annotation.Order;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;

import com.techmatters.common.spring.configuration.WebSecurityConfiguration.LiveAuthorizationConfiguration;

@Configuration
@EnableWebSecurity
@Profile({"default","dev","local-test","dev-demo","test","uat","live"})
@Order(2)
public class LiveWebSecurityConfiguration  extends LiveAuthorizationConfiguration {

	@Override
	protected String[] getUnauthenticatedPaths() {
		return new String[] {"/", "/dashboard", "/insight"};
	}
	
	@Override
	protected String[] getAuthenticatedPaths() {
		return new String[] {"/secured/**"};
	}
}