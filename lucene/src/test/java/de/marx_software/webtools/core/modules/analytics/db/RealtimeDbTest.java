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
import com.alibaba.fastjson.JSONObject;
import java.util.concurrent.Future;
import static org.assertj.core.api.Assertions.*;
import org.testng.annotations.Test;
import de.marx_software.webtools.api.analytics.Fields;
import de.marx_software.webtools.api.analytics.query.Aggregator;
import de.marx_software.webtools.api.analytics.query.Query;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;

/**
 *
 * @author thmarx
 */
public class RealtimeDbTest {

	DefaultAnalyticsDb instance;
	MockedExecutor executor = new MockedExecutor();

	@BeforeClass
	public void setup() {
		Configuration config = TestHelper.getConfiguration("target/RealtimeDbTest-" + System.currentTimeMillis());

		instance = new DefaultAnalyticsDb(config, executor);

		instance.open();
	}

	@AfterClass
	public void tearDown() throws InterruptedException, Exception {
		instance.close();
		executor.shutdown();
	}
	
	@DataProvider(name = "steps")
   public static Object[] steps() {
      return new Object[] {1, 6, 19, 22};
   }

	/**
	 *
	 *
	 * Test of open method, of class AnalyticsDb.
	 */
	@Test(enabled = true, invocationCount = 2, dataProvider = "steps")
	public void test_realtime(final int step) throws Exception {

		System.out.println("running realtime test : step size " + step);
		long before = System.currentTimeMillis();

		long count = instance.index().size();

		assertThat(query_size()).isEqualTo(count);
		assertThat(instance.index().size()).isEqualTo(count);
		
		count += track(1);
		assertThat(query_size()).isEqualTo(count);
		assertThat(instance.index().size()).isEqualTo(count);
		
		for (int i = 0; i < 100; i++) {
			count += track(step);
			assertThat(query_size()).isEqualTo(count);
			assertThat(instance.index().size()).isEqualTo(count);
		}
		
		long after = System.currentTimeMillis();
		
		System.out.println((after - before) + "ms");
	}

	private int track (final int count) {
		for (int i = 0; i < count; i++) {
			instance.track(TestHelper.event(event(), new JSONObject()));
		}
		return count;
	}
	
	private int query_size () throws InterruptedException, ExecutionException {
		long startTime = System.currentTimeMillis() - (1000 * 60 * 60);
		long endTime = System.currentTimeMillis() + (1000 * 60 * 60);
		Query query = Query.builder().start(startTime).end(endTime).build();
		Future<Integer> future = instance.query(query, new Aggregator<Integer>() {
			@Override
			public Integer call() throws Exception {
				return documents.size();
			}
		});
		
		return future.get();
	}
	
	private JSONObject event() {

		long timestamp = System.currentTimeMillis();

		JSONObject event = new JSONObject();

		event.put(Fields._TimeStamp.value(), timestamp);
		event.put("ua", "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:51.0) Gecko/20100101 Firefox/51.0");
		event.put(Fields._UUID.value(), UUID.randomUUID().toString());
		event.put(Fields.UserId.value(), UUID.randomUUID().toString());
		event.put(Fields.VisitId.value(), UUID.randomUUID().toString());
		event.put(Fields.RequestId.value(), UUID.randomUUID().toString());

		return event;
	}
}
