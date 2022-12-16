package uk.gov.justice.dpr.application.controller;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
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
	
	@RequestMapping(value={"/dpr-132"}, method=RequestMethod.GET)
	public ModelAndView getDPR132Interactive() {
		final ModelAndView mav = new ModelAndView("/site/dpr-132");
		
		try {
			// establishments
			final Map<String,String> establishments = convertResult(statsService.executeRaw("select id, name from domain.establishment order by name" ), "id", "name");
			mav.addObject("establishments", establishments);
			// types
			final Map<String,String> types = new HashMap<String,String>();
			types.put("use_of_force", "Use of Force");
			mav.addObject("types", types);
		} catch(Exception e) {
			mav.addObject("establishments", new HashMap<String,String>());
			mav.addObject("types", new HashMap<String,String>());
			mav.addObject("exception", getStackTrace(e));
		}
		return mav;
	}
	
	@RequestMapping(value={"/app/interactive"}, method=RequestMethod.POST, produces="text/plain")
	public @ResponseBody String runInteractive(final HttpServletRequest request, @RequestParam Map<String,String> params) throws JsonProcessingException {
		// the query is going to be standard
		final String select = "select id,to_char(to_timestamp(incident_date,'YYYY-MM-DD\"H\"HH24:MI:SS\"Z\"'),'DD-MM-YYYY   HH24:MI'),type,offender_no,location,age_at_incident,age_category from mv_aged_incident_binned ";
		String where = " where date(incident_date) between ? and ? ";
		String typePred = " AND type = ? ";
		String estPred = " AND location=? ";
		String orderBy =  "order by date(incident_date) desc";
		
		String query = select + where;
		List<Object> queryParams = new ArrayList<Object>();
		
		// date processing
		String fromDate = params.get("start");
		String toDate = params.get("end");
		if(StringUtils.isEmpty(fromDate)) {
			fromDate = "0001-01-01";
		} else {
			fromDate = DateTime.parse(fromDate, DateTimeFormat.forPattern("dd/MM/yyyy") ).toString("yyyy-MM-dd");
		}
		if(StringUtils.isEmpty(toDate)) {
			toDate = "9999-12-31";
		} else {
			toDate = DateTime.parse(toDate, DateTimeFormat.forPattern("dd/MM/yyyy") ).toString("yyyy-MM-dd");
		}
		queryParams.add(fromDate);
		queryParams.add(toDate);
		
		// establishment processing
		if(!StringUtils.isEmpty(params.get("establishment"))) {
			query += estPred;
			queryParams.add(params.get("establishment"));
		}
		// type processing
		if(!StringUtils.isEmpty(params.get("type"))) {
			query += typePred;
			queryParams.add(params.get("type"));
		}
		
		query += orderBy;
		
		final List<Map<String, Object>> results  = statsService.executeQueryWithParameters(query, queryParams.toArray());
		return MAPPER.writeValueAsString(results);
	}
	
	@RequestMapping(value={"/app/query"}, method=RequestMethod.POST, produces="text/plain")
	public @ResponseBody String runQuery(final HttpServletRequest request, @RequestParam Map<String,String> params) throws JsonProcessingException {
		final String query = params.get("query");
		final List<Map<String, Object>> results = statsService.executeRaw(query);
		return MAPPER.writeValueAsString(results);
	}
	
	@RequestMapping(value={"/insight/{id}"}, method=RequestMethod.GET)
	public ModelAndView getInsight(@PathVariable("id") final String id) {
		return new ModelAndView("/site/insight");
	}
	
	protected Map<String, String> convertResult(List<Map<String,Object>> items, final String key, final String value) {
		Map<String, String> results = new HashMap<String,String>();
		for(final Map<String,Object> item : items) {
			final String k = item.get(key).toString();
			final String v = item.get(value).toString();
			results.put(k, v);
		}
		return results;
	}
	
	protected static String getStackTrace(final Exception e) {
		final StringWriter sw = new StringWriter();
		final PrintWriter pw = new PrintWriter(sw);
		e.printStackTrace(pw);
		return sw.getBuffer().toString();
	}
}
