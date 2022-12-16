package uk.gov.justice.dpr.application.service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.sql.DataSource;

import org.joda.time.DateTime;
import org.joda.time.Period;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;

import uk.gov.justice.dpr.application.model.Statistic;
import uk.gov.justice.dpr.application.query.QueryStore;

@Service
public class StatisticsService {

	private final JdbcTemplate template;
	private final static StatsRowMapper statsMapper = new StatsRowMapper();
		
	@Autowired
	public StatisticsService(@Qualifier("datamart-source") final DataSource source) {
		this.template = new JdbcTemplate(source);
	}
	
	public List<Statistic> getStatisticsForLocation(final String location) {
		List<Statistic> stats = getRangeStats(location, new DateTime(0), Period.years(9999));
		return stats;
	}
	
	public List<Map<String,Object>> executeRaw(final String sql) {
		return template.queryForList(sql);
	}
	
	public List<Map<String,Object>> executeQueryWithParameters(final String sql, final Object...args) {
		return template.queryForList(sql, args);
	}
	
	protected List<Statistic> getRangeStats(final String location, final DateTime fromDate, final Period period) {
		List<Statistic> stats = new ArrayList<Statistic>();
		
		final DateTime toDate = fromDate.plus(period);
		final String duration = getDurationNameFromPeriod(period);
		
		// dummy stats for now
		stats.add(createStat("Population", getIntValue(1100, 1121), duration, getValue(-4.1, 3.2), "person"));
		stats.add(createStat("Assaults", getIntValue(0, 12), duration, getValue(-4.1, 3.2), "people-robbery"));
		stats.add(createStat("Self harm", getIntValue(0, 12), duration, getValue(-4.1, 3.2), "bandage"));
		stats.add(createStat("Finds", getIntValue(0, 12), duration, getValue(-4.1, 3.2), "magnifying-glass"));
		stats.add(createStat("Disorder", getIntValue(0, 12), duration, getValue(-4.1, 3.2), "person-harassing"));
		try {
			stats.add(getStat(QueryStore.getQuery("use-of-force"), "Use of force", duration, "person-military-to-person", fromDate.toDate(), toDate.toDate()));
		} catch(Exception e) {
			stats.add(createStat("Use of force", getIntValue(0, 12), duration, getValue(-4.1, 3.2), "person-military-to-person"));
		}
		return stats;
	}
	
	protected String getDurationNameFromPeriod(final Period period) {
		if(period.getDays() == 1)
			return "day";
		if(period.getWeeks() == 1) 
			return "week";
		if(period.getMonths() == 1)
			return "month";
		if(period.getMonths() == 3)
			return "quarter";
		if(period.getYears() ==1) 
			return "year";
		
		return "period";
	}
	
	
	public Statistic getStat(final String sql, final String name, final String duration, final String icon, Object...args) {
		// get values
		List<DateStats> results = template.query(sql, statsMapper); //, args);
		
		if(results.isEmpty()) {
			return createEmptyStat(name, duration, icon);
		} else {
			Statistic stat = new Statistic();
			stat.setName(name);
			stat.setDuration(duration);
			stat.setIcon(icon);
			switch(results.size()) {
			case 1:
				stat.setValue(results.get(0).getCount());
				stat.setPc(0);
				break;
			default:
				long newValue = results.get(0).getCount();
				long oldValue = results.get(1).getCount();
				stat.setValue(newValue);
				stat.setPc((newValue-oldValue) * 100.0/oldValue);
				break;
			}
			return stat;
		}
	}
	
	protected Statistic createEmptyStat(final String name, final String duration, final String icon) {
		Statistic stat = new Statistic();
		stat.setName(name);
		stat.setValue(0);
		stat.setDuration(duration);
		stat.setPc(0);
		stat.setIcon(icon);
		
		return stat;
	}
	
	protected Statistic createStat(final String name, final double value, final String duration, final double pc, final String icon) {
		Statistic stat = new Statistic();
		stat.setName(name);
		stat.setValue(value);
		stat.setDuration(duration);
		stat.setPc(pc);
		stat.setIcon(icon);
		
		return stat;
	}
	
	private Random rand = new Random();
	
	protected double getValue(double min, double max) {
		return min + (max - min) * rand.nextDouble();
	}
	
	protected double getIntValue(int min, int max) {
		return rand.nextInt(max + 1 - min) + min;
	}
	
	private static class DateStats {
		private DateTime date;
		private long count;
		
		public DateStats(final DateTime date, final long count) {
			this.date = date;
			this.count = count;
		}
		
		@SuppressWarnings("unused")
		public DateTime getDate() {
			return date;
		}
		public long getCount() {
			return count;
		}
		
	}
	
	public static class StatsRowMapper implements RowMapper<DateStats> {
		@Override
		public DateStats mapRow(ResultSet rs, int rowNum) throws SQLException {	
			return new DateStats(new DateTime(rs.getTimestamp("week")), rs.getLong("count"));
		}

	}
}
