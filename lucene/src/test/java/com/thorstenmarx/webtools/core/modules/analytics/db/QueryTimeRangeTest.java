package com.thorstenmarx.webtools.core.modules.analytics.db;

/*-
 * #%L
 * webtools-analytics
 * %%
 * Copyright (C) 2016 - 2018 Thorsten Marx
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/gpl-3.0.html>.
 * #L%
 */
import com.alibaba.fastjson.JSONObject;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import static org.assertj.core.api.Assertions.*;
import org.testng.annotations.Test;
import com.thorstenmarx.webtools.api.analytics.Fields;
import com.thorstenmarx.webtools.api.analytics.query.Aggregator;
import com.thorstenmarx.webtools.api.analytics.query.Query;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

/**
 *
 * @author thmarx
 */
public class QueryTimeRangeTest {

	DefaultAnalyticsDb instance;
	MockedExecutor executor = new MockedExecutor();
	
	@BeforeClass
	public void setup() {
		Configuration config = TestHelper.getConfiguration("target/QueryTimeRangeTest-" + System.currentTimeMillis());
		
		instance = new DefaultAnalyticsDb(config, executor);

		instance.open();
	}

	@AfterClass
	public void tearDown() throws InterruptedException, Exception {
		instance.close();
		executor.shutdown();
	}

	/**
	 * Test of open method, of class AnalyticsDb.
	 */
	@Test()
	public void testAnalyticsDb() throws Exception {

		System.out.println("query by userid");

		long timestamp = System.currentTimeMillis();
		JSONObject data = new JSONObject();
		final String userid = "87a81d7c-ff5b-40dc-b653-3f85ef6e9f71";
//		final String userid = "user1";
//		event.put(Shard.Field.TIMESTAMP.value(), ZonedDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
		data.put(Fields._TimeStamp.value(), 1000l);
		data.put("ua", "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:51.0) Gecko/20100101 Firefox/51.0");
		data.put(Fields.UserId.value(), userid);
		
		instance.track(TestHelper.event(data, new JSONObject()));

		System.out.println("query lower range");
		Query query = Query.builder().start(0).end(900).term(Fields.UserId.value(), userid).build();
		Future<Map<String, Object>> future = instance.query(query, new Aggregator<Map<String, Object>>() {
			@Override
			public Map<String, Object> call() throws Exception {
				Map<String, Object> result = new HashMap<>();
				result.put("count", documents.size());
				return result;
			}
		});

		Map<String, Object> result = future.get();
		System.out.println(result);
		assertThat((int) result.get("count")).isEqualTo(0);

		System.out.println("query upper range");
		query = Query.builder().start(2000).end(3000).term(Fields.UserId.value(), userid).build();
		future = instance.query(query, new Aggregator<Map<String, Object>>() {
			@Override
			public Map<String, Object> call() throws Exception {
				Map<String, Object> result = new HashMap<>();
				result.put("count", documents.size());
				return result;
			}
		});

		result = future.get();
		System.out.println(result);
		assertThat((int) result.get("count")).isEqualTo(0);
		
		
		System.out.println("query matching range");
		query = Query.builder().start(0).end(Long.MAX_VALUE).term(Fields.UserId.value(), userid).build();
		future = instance.query(query, new Aggregator<Map<String, Object>>() {
			@Override
			public Map<String, Object> call() throws Exception {
				Map<String, Object> result = new HashMap<>();
				result.put("count", documents.size());
				return result;
			}
		});

		result = future.get();
		System.out.println(result);
		assertThat((int) result.get("count")).isEqualTo(1);
		
	}

}
