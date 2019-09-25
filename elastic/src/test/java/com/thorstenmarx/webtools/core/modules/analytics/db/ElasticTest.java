/*
 * Copyright (C) 2019 Thorsten Marx
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.thorstenmarx.webtools.core.modules.analytics.db;

/*-
 * #%L
 * webtools-analytics-elastic
 * %%
 * Copyright (C) 2016 - 2019 Thorsten Marx
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
import com.thorstenmarx.webtools.api.analytics.Fields;
import com.thorstenmarx.webtools.api.analytics.query.Aggregator;
import com.thorstenmarx.webtools.api.analytics.query.Query;
import com.thorstenmarx.webtools.core.modules.analytics.db.index.elastic.TestHelper;
import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;
import java.util.Map;
import java.util.concurrent.Future;
import net.engio.mbassy.bus.MBassador;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 *
 * @author marx
 */
public class ElasticTest {

	RestHighLevelClient restHighLevelClient;
	private String indexName;

	@BeforeClass
	public void setup() throws IOException {
		restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200)));

		indexName = "analytics" + System.currentTimeMillis();

		CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
		CreateIndexResponse response = restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
		System.out.println(response.index());
	}

	@AfterClass
	public void tearDown() throws IOException {
		restHighLevelClient.close();
	}

	/**
	 * Test of open method, of class AnalyticsDb.
	 */
	@Test(enabled = false)
	public void testAnalyticsDb() throws Exception {

		System.out.println("running analytics db test");

		Configuration config = new Configuration(indexName, "localhost:9200");

		try (ElasticAnalyticsDb instance = new ElasticAnalyticsDb(config, new MBassador(), restHighLevelClient)) {
			instance.open();

			JSONObject data = new JSONObject();

			long timestamp = System.currentTimeMillis();
			data.put(Fields._TimeStamp.value(), timestamp);
			data.put("ua", "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:51.0) Gecko/20100101 Firefox/51.0");
			data.put(Fields._UUID.value(), UUID.randomUUID().toString());

			JSONObject test = new JSONObject();
			test.put("name", "klaus_testAnalyticsDb");
			data.put("test", test);

			data.put("age", 25);
			data.put("average", 25.5);
			data.put("event", "pageLoaded");

			JSONObject meta = new JSONObject();
			meta.put("ip", "88.153.198.210");

			System.out.println(instance.index().size());
			instance.track(TestHelper.event(data, meta));
			System.out.println(instance.index().size());

			Query query = Query.builder().term("event", "pageLoaded")
					.start(0).end(timestamp + 50000)
					.build();
			Future<Map<String, Object>> future = instance.query(query, new Aggregator<Map<String, Object>>() {
				@Override
				public Map<String, Object> call() throws Exception {
					Map<String, Object> result = new HashMap<>();
					result.put("count", documents.size());
					return result;
				}
			});

			Map<String, Object> result = future.get();
			System.out.println(result.get("count"));
		}
	}

	@Test(enabled = false)
	public void simple_performance_test() throws Exception {

		System.out.println("running analytics db test");

		Configuration config = new Configuration(indexName, "localhost:9200");

		try (ElasticAnalyticsDb instance = new ElasticAnalyticsDb(config, new MBassador(), restHighLevelClient)) {
			instance.open();

			final long before = System.currentTimeMillis();
			for (int i = 0; i < 10000; i++) {
				JSONObject data = new JSONObject();

				long timestamp = System.currentTimeMillis();
				data.put(Fields._TimeStamp.value(), timestamp);
				data.put("ua", "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:51.0) Gecko/20100101 Firefox/51.0");
				data.put(Fields._UUID.value(), UUID.randomUUID().toString());

				JSONObject test = new JSONObject();
				test.put("name", "klaus_testAnalyticsDb");
				data.put("test", test);

				data.put("age", 25);
				data.put("average", 25.5);
				data.put("event", "pageLoaded");

				JSONObject meta = new JSONObject();
				meta.put("ip", "88.153.198.210");

				instance.track(TestHelper.event(data, meta));
			}
			final long after = System.currentTimeMillis();

			System.out.println((after - before) + " ms");
		}
	}
}
