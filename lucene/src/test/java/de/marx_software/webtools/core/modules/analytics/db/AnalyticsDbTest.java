package de.marx_software.webtools.core.modules.analytics.db;

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
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import static org.assertj.core.api.Assertions.*;
import org.testng.annotations.Test;
import de.marx_software.webtools.api.analytics.Fields;
import de.marx_software.webtools.api.analytics.query.Aggregator;
import de.marx_software.webtools.api.analytics.query.Query;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

/**
 *
 * @author thmarx
 */
public class AnalyticsDbTest {

	DefaultAnalyticsDb instance;
	MockedExecutor executor = new MockedExecutor();
	
	@BeforeClass
	public void setup() {
		Configuration config = TestHelper.getConfiguration("target/AnalyticsDbTest-" + System.currentTimeMillis());
		
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

		System.out.println("running analytics db test");

		long timestamp = System.currentTimeMillis();
		JSONObject data = new JSONObject();

//		event.put(Shard.Field.TIMESTAMP.value(), ZonedDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
		data.put(Fields._TimeStamp.value(), timestamp);
		data.put("ua", "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:51.0) Gecko/20100101 Firefox/51.0");
		data.put(Fields._UUID.value(), UUID.randomUUID().toString());

		JSONObject test = new JSONObject();
		test.put("name", "klaus_testAnalyticsDb");
		data.put("test", test);

		data.put("age", 25);
		data.put("average", 25.5);
		data.put("event", "pageLoaded_testAnalyticsDb");
		
		JSONObject meta = new JSONObject();
		meta.put("ip", "88.153.198.210");

		long count = instance.index().size();
		System.out.println(instance.index().size());
		instance.track(TestHelper.event(data, meta));
		System.out.println(instance.index().size());
		
		Awaitility.await().atMost(20, TimeUnit.SECONDS).until((Callable<Boolean>) () -> {
			System.out.println((count + 1) + "/" + instance.index().size());
			return true;
		});

//		instance.index().reopen();
//		Thread.sleep(5000);
		long startTime = timestamp - (1000 * 60 * 60);
		long endTime = timestamp + (1000 * 60 * 60);
		Query query = Query.builder().start(startTime).end(endTime).term("event", "pageLoaded_testAnalyticsDb").build();
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
		assertThat((int) result.get("count")).isEqualTo(1);

		query = Query.builder().start(startTime).end(endTime).term("event", "click_testAnalyticsDb").build();
		future = instance.query(query, new Aggregator<Map<String, Object>>() {
			@Override
			public Map<String, Object> call() throws Exception {
				Map<String, Object> result = new HashMap<>();
				result.put("count", documents.size());
				return result;
			}
		});

		result = future.get();
		assertThat((int) result.get("count")).isEqualTo(0);

		query = Query.builder().start(startTime).end(endTime).term("test.name", "klaus_testAnalyticsDb").build();
		future = instance.query(query, new Aggregator<Map<String, Object>>() {
			@Override
			public Map<String, Object> call() throws Exception {
				Map<String, Object> result = new HashMap<>();
				result.put("count", documents.size());
				return result;
			}
		});

		result = future.get();
		assertThat((int) result.get("count")).isEqualTo(1);
	}

	@Test()
	public void testMap() throws Exception {

		System.out.println("running analytics db map test");

		long timestamp = System.currentTimeMillis();
		long count = instance.index().size();

		Map<String, Object> data = new HashMap<>();

//		event.put(Shard.Field.TIMESTAMP.value(), ZonedDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
		data.put(Fields._TimeStamp.value(), timestamp);
		data.put("ua", "Mozilla/5.0 (Windows NT 6.3; Win64; x64; rv:38.0) Gecko/20100101 Firefox/38.0");
		data.put(Fields._UUID.value(), UUID.randomUUID().toString());

		Map<String, Object> test = new HashMap<>();
		test.put("name", "peter_testMap");
		data.put("test", test);

		data.put("age", 25);
		data.put("average", 25.5);
		data.put("event", "pageView_testMap");
		
		Map<String, Object> meta = new HashMap<>();
		meta.put("ip", "88.153.198.210");

		instance.track(TestHelper.event(data, meta));

//		Thread.sleep(5000);
		Awaitility.await().atMost(5, TimeUnit.SECONDS).until((Callable<Boolean>) () -> instance.index().size() >= (count + 1));

		long startTime = timestamp - (1000 * 60 * 60);
		long endTime = timestamp + (1000 * 60 * 60);
		Query query = Query.builder().start(startTime).end(endTime).term("event", "pageView_testMap").build();
		Future<Map> future = instance.query(query, new Aggregator<Map>() {
			@Override
			public Map call() throws Exception {
				Map<String, Object> result = new HashMap<>();
				result.put("count", documents.size());
				return result;
			}
		});

		Map<String, Object> result = future.get();
		assertThat((int) result.get("count")).isEqualTo(1);

		query = Query.builder().start(startTime).end(endTime).term("event", "click_testMap").build();
		future = instance.query(query, new Aggregator<Map>() {
			@Override
			public Map call() throws Exception {
				Map<String, Object> result = new HashMap<>();
				result.put("count", documents.size());
				return result;
			}
		});

		result = future.get();
		assertThat((int) result.get("count")).isEqualTo(0);

		query = Query.builder().start(startTime).end(endTime).term("test.name", "peter_testMap").build();
		future = instance.query(query, new Aggregator<Map>() {
			@Override
			public Map call() throws Exception {
				Map<String, Object> result = new HashMap<>();
				result.put("count", documents.size());
				return result;
			}
		});

		result = future.get();
		assertThat((int) result.get("count")).isEqualTo(1);
	}

	@Test()
	public void testQuery() throws Exception {

		System.out.println("running testQuery");

		long timestamp = System.currentTimeMillis();
		long count = instance.index().size();

		Map<String, Object> event = new HashMap<>();

		event.put(Fields._TimeStamp.value(), timestamp);
		event.put("ua", "Mozilla/5.0 (Windows NT 6.3; Win64; x64; rv:38.0) Gecko/20100101 Firefox/38.0");
		event.put(Fields._UUID.value(), UUID.randomUUID().toString());

		Map<String, Object> test = new HashMap<>();
		test.put("name", "peter_testQuery");
		event.put("test", test);

		event.put("age", 25);
		event.put("average", 25.5);
		event.put("event", "pageView_testQuery");
		
		
		Map<String, Object> meta = new HashMap<>();
		meta.put("ip", "88.153.198.210");

		instance.track(TestHelper.event(event, meta));

//		Thread.sleep(1000);
		Awaitility.await().atMost(60, TimeUnit.SECONDS).until((Callable<Boolean>) () -> instance.index().size() >= (count + 1));

		long startTime = timestamp - (1000 * 60 * 60);
		long endTime = timestamp + (1000 * 60 * 60);
		Query query = Query.builder().start(startTime).end(endTime).term("event", "pageView_testQuery").build();
		Future<Map> future = instance.query(query, new Aggregator<Map>() {
			@Override
			public Map call() throws Exception {
				Map<String, Object> result = new HashMap<>();
				result.put("count", documents.size());
				int age = documents.get(0).document.getIntValue("age");
				return result;
			}
		});

		Map<String, Object> result = future.get();
		assertThat((int) result.get("count")).isEqualTo(1);
	}
	
	@Test()
	public void test_exits() throws Exception {

		System.out.println("running testQuery");

		long timestamp = System.currentTimeMillis();

		Map<String, Object> event = new HashMap<>();

		event.put(Fields._TimeStamp.value(), timestamp);
		final String uuid = UUID.randomUUID().toString();
		event.put(Fields._UUID.value(), uuid);

		Map<String, Object> meta = new HashMap<>();

		instance.track(TestHelper.event(event, meta));

		Assertions.assertThat(instance.exists(uuid)).isTrue();
	}
	
	@Test()
	public void test_exits_not() throws Exception {

		System.out.println("test_exits_not");

		final String uuid = UUID.randomUUID().toString();

		Assertions.assertThat(instance.exists(uuid)).isFalse();
	}

	@Test()
	public void testQueryMultivalueterms() throws Exception {

		System.out.println("running testQueryMultivalueterms");

		long timestamp = System.currentTimeMillis();

		Map<String, Object> event = new HashMap<>();

		long count = instance.index().size();
		
		event.put(Fields._TimeStamp.value(), timestamp);
		event.put("ua", "Mozilla/5.0 (Windows NT 6.3; Win64; x64; rv:38.0) Gecko/20100101 Firefox/38.0");
		event.put(Fields._UUID.value(), UUID.randomUUID().toString());
		Map<String, Object> test = new HashMap<>();
		test.put("name", "peter_testQueryMultivalueterms");
		event.put("test", test);
		event.put("age", 25);
		event.put("average", 25.5);
		event.put("event", "pageView_testQueryMultivalueterms");
		
		Map<String, Object> meta = new HashMap<>();
		meta.put("ip", "88.153.198.210");

		instance.track(TestHelper.event(event, meta));

		test = new HashMap<>();
		test.put("name", "klaus_testQueryMultivalueterms");
		event.put("test", test);
		event.put(Fields._UUID.value(), UUID.randomUUID().toString());
		instance.track(TestHelper.event(event, meta));

//		Thread.sleep(5000);
		Awaitility.await().atMost(60, TimeUnit.SECONDS).until((Callable<Boolean>) () -> {
			return instance.index().size() >= (count+2);
		});

		long startTime = timestamp - (1000 * 60 * 60);
		long endTime = timestamp + (1000 * 60 * 60);
		Query query = Query.builder().start(startTime).end(endTime)
				.multivalueTerm("test.name", new String[]{"peter_testQueryMultivalueterms"}).build();
		Future<Map> future = instance.query(query, new Aggregator<Map>() {
			@Override
			public Map call() throws Exception {
				Map<String, Object> result = new HashMap<>();
				result.put("count", documents.size());
				int age = documents.get(0).document.getIntValue("age");
				return result;
			}
		});

		Map<String, Object> result = future.get();
		assertThat((int) result.get("count")).isEqualTo(1);

		query = Query.builder().start(startTime).end(endTime)
				.multivalueTerm("test.name", new String[]{"peter_testQueryMultivalueterms", "klaus_testQueryMultivalueterms"}).build();
		future = instance.query(query, new Aggregator<Map>() {
			@Override
			public Map call() throws Exception {
				Map<String, Object> result = new HashMap<>();
				result.put("count", documents.size());
				int age = documents.get(0).document.getIntValue("age");
				return result;
			}
		});

		result = future.get();
		assertThat((int) result.get("count")).isEqualTo(2);
	}

	@Test
	public void testFlatJson() {
		JSONObject event = new JSONObject();

//		event.put(Shard.Field.TIMESTAMP.value(), ZonedDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
		event.put(Fields._TimeStamp.value(), System.currentTimeMillis());
		JSONObject location = new JSONObject();
		location.put("lat", -88.21337);
		location.put("lng", 40.11041);
		event.put("location", location);

		JSONObject test = new JSONObject();
		test.put("name", "klaus");
		JSONArray arr = new JSONArray();
		arr.add("eins");
		arr.add("zwei");
		arr.add(test);
		event.put("test", arr);

		event.put("age", 25);
		event.put("average", 25.5);
		event.put("event", "pageLoaded");
		event.put("ip", "88.153.198.210");
		event.put("ua", "Mozilla/5.0 (Windows NT 6.3; Win64; x64; rv:38.0) Gecko/20100101 Firefox/38.0");

		event.put("alist", new String[]{"eins", "zwei", "drei"});

		flatJsonObject(null, event);

		System.out.println(event.toJSONString());
	}

	private void flatJsonObject(final String name, final JSONObject json) {
		json.keySet().stream().forEach((key) -> {
			String localname = name != null ? (name + ".") : "";
			localname += key;
			Object value = json.get(key);
			if (value instanceof JSONArray) {
				JSONArray array = (JSONArray) value;
				flatJsonArray(localname, array);
			} else if (value instanceof JSONObject) {
				flatJsonObject(localname, (JSONObject) value);
			} else {
				handleItem(localname, value);
			}
		});
	}

	private void flatJsonArray(final String name, final JSONArray array) {
		array.stream().forEach((item) -> {
			if (item instanceof JSONArray) {
				flatJsonArray(name, (JSONArray) item);
			} else if (item instanceof JSONObject) {
				flatJsonObject(name, (JSONObject) item);
			} else {
				handleItem(name, item);
			}
		});
	}

	private void handleItem(String name, Object value) {
		System.out.println(name + " : " + value + " / " + value.getClass());
	}

}
