package uk.gov.justice.dpr.application.configuration;


import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.annotation.Order;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;


@Configuration
@EnableWebSecurity
@Profile({"default","dev","local-test","dev-demo","test","uat","live"})
@Order(2)
public class LiveWebSecurityConfiguration extends WebSecurityConfigurerAdapter {

	protected String[] getUnauthenticatedPaths() {
		return new String[] {"/", "/dashboard", "/insight"};
	}
	
	protected String[] getAuthenticatedPaths() {
		return new String[] {"/secured/**"};
	}
	
	protected String[] getOnboardingPaths() {
		return new String[] {"/authenticate","/password-retrieve","/password-reset","/signup", "/complete", "/auth/**"};
	}
	
	protected String[] getAssetPaths() {
		return new String[] {"/assets/**", "/favicon.ico", "/images/**"};
	}

	/**
	 * This configuration provides a security for live services
	 *
	 */
	@Override
    protected void configure(HttpSecurity http) throws Exception {
		
        http
        		
                .authorizeRequests()
                
                 // onboarding paths
                .antMatchers(getOnboardingPaths()).permitAll()
                 // decoration paths
                .antMatchers(getAssetPaths()).permitAll()
                // unauthenticated paths
                .antMatchers(getUnauthenticatedPaths()).permitAll()
                 // admin paths
                .antMatchers("/admin/ping").permitAll()
                
                 // otherwise must be authenticated
                .antMatchers(getAuthenticatedPaths()).authenticated()
                
                .and()
                	.formLogin()
                	.loginPage("/authenticate")
                		.permitAll()
                	.loginProcessingUrl("/login")
                	.defaultSuccessUrl("/")
                
	            .and()
	                .logout().invalidateHttpSession(true).deleteCookies("JSESSIONID")
	                
                .and()
                	.rememberMe()
                		.useSecureCookie(true)
                		.tokenValiditySeconds(15552000)
                
                .and()
	                .csrf()
	                	.disable();
	             
    }
	
}