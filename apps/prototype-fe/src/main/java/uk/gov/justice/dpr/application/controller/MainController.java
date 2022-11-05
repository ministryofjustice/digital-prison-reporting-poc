package uk.gov.justice.dpr.application.controller;

import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.ModelAndView;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import uk.gov.justice.dpr.application.query.QueryStore;
import uk.gov.justice.dpr.application.service.StatisticsService;

@Controller
public class MainController {
	
	final static ObjectMapper MAPPER = new ObjectMapper();
	
	final StatisticsService statsService;
	
	@Autowired
	public MainController(final StatisticsService statsService) {
		this.statsService = statsService;
	}
	

	@RequestMapping(value={"/"}, method=RequestMethod.GET)
	public ModelAndView homePage() {
		return new ModelAndView("/site/home");
	}
	
	@RequestMapping(value={"/dashboard"}, method=RequestMethod.GET)
	public ModelAndView getDashboard() {
		final ModelAndView mav = new ModelAndView("/site/dashboard");
		// fill with statistics
		mav.addObject("summary", statsService.getStatisticsForLocation(""));
		return mav;
	}
	
	@RequestMapping(value={"/playground"}, method=RequestMethod.GET)
	public ModelAndView getPlayground() {
		final ModelAndView mav = new ModelAndView("/site/playground");
		// fill with statistics
		mav.addObject("queries", QueryStore.allQueries());
		return mav;
	}
	
	@RequestMapping(value={"/app/query"}, method=RequestMethod.POST, produces="text/plain")
	public @ResponseBody String runPrediction(final HttpServletRequest request, @RequestParam Map<String,String> params) throws JsonProcessingException {
		final String query = params.get("query");
		final List<Map<String, Object>> results = statsService.executeRaw(query);
		return MAPPER.writeValueAsString(results);
	}
	
	@RequestMapping(value={"/insight/{id}"}, method=RequestMethod.GET)
	public ModelAndView getInsight(@PathVariable("id") final String id) {
		return new ModelAndView("/site/insight");
	}
}
