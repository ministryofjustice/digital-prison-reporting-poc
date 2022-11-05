package uk.gov.justice.dpr.application.servlet;

import javax.servlet.ServletException;

import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

import com.opensymphony.module.sitemesh.freemarker.FreemarkerDecoratorServlet;

import freemarker.cache.TemplateLoader;
import freemarker.template.Configuration;

public class SpringFreemarkerDecoratorServlet extends FreemarkerDecoratorServlet {

	private static final long serialVersionUID = -191341511755985383L;

	@Override
	public void init() throws ServletException {
		super.init();
		WebApplicationContext ctx = WebApplicationContextUtils.getRequiredWebApplicationContext(getServletContext());
		Configuration springConfiguration = (Configuration) ctx.getBean(Configuration.class);
		TemplateLoader templateLoader = springConfiguration.getTemplateLoader();
		getConfiguration().setTemplateLoader(templateLoader);
	}
}
