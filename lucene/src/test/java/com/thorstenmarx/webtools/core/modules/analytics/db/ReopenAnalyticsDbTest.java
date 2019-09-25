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
import org.testng.annotations.Test;
import com.thorstenmarx.webtools.api.analytics.Fields;
import java.util.UUID;
import net.engio.mbassy.bus.MBassador;
import org.assertj.core.api.Assertions;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;


/**
 *
 * @author thmarx
 */
public class ReopenAnalyticsDbTest {

	DefaultAnalyticsDb instance;
	MockedExecutor executor;		
	Configuration config;
	
	@BeforeMethod
	public void open () {
		config = new Configuration("target/ReopenAnalyticsDbTest-" + System.currentTimeMillis());

		executor = new MockedExecutor();
		instance = new DefaultAnalyticsDb(config, executor);

		instance.open();
	}
	@AfterMethod
	public void close () throws InterruptedException, Exception {
		instance.close();
		executor.shutdown();
	}
	
	private void reopen () throws Exception {
		instance.close();
		executor.shutdown();
		
		executor = new MockedExecutor();
		instance = new DefaultAnalyticsDb(config, executor);

		instance.open();
	}
	
	/**
	 * Test of open method, of class AnalyticsDb.
	 */
	@Test()
	public void test_AnalyticsDb_reopen() throws Exception {

		System.out.println("running test_AnalyticsDb_reopen");

		

		long timestamp = System.currentTimeMillis();
		JSONObject event = new JSONObject();

		
//		event.put(Shard.Field.TIMESTAMP.value(), ZonedDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
		event.put(Fields._TimeStamp.value(), timestamp);
		event.put("ua", "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:51.0) Gecko/20100101 Firefox/51.0");
		event.put(Fields._UUID.value(), UUID.randomUUID().toString());
		

		JSONObject test = new JSONObject();
		test.put("name", "klaus");
		event.put("test", test);

		event.put("age", 25);
		event.put("average", 25.5);
		event.put("event", "pageLoaded");
		
		JSONObject meta = new JSONObject();
		meta.put("ip", "88.153.198.210");
		
		instance.track(TestHelper.event(event, new JSONObject()));

		
		reopen();
		
		Assertions.assertThat(instance.index().size()).isEqualTo(1);
		
		for (int i = 0; i < 200; i++) {
			event.put(Fields._UUID.value(), UUID.randomUUID().toString());
			instance.track(TestHelper.event(event, new JSONObject()));
		}
		
		Assertions.assertThat(instance.index().size()).isEqualTo(201);
		
		
		reopen();
		
		Assertions.assertThat(instance.index().size()).isEqualTo(201);
		
		Thread.sleep(5000);
		
		Assertions.assertThat(instance.index().size()).isEqualTo(201);
		
		reopen();
		
		Assertions.assertThat(instance.index().size()).isEqualTo(201);
	}
	
	@Test(invocationCount = 2)
	public void test_AnalyticsDb_reopen_big() throws Exception {

		System.out.println("running analytics db test");

		

		long timestamp = System.currentTimeMillis();
		JSONObject event = new JSONObject();

		
//		event.put(Shard.Field.TIMESTAMP.value(), ZonedDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
		event.put(Fields._TimeStamp.value(), timestamp);
		event.put("ua", "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:51.0) Gecko/20100101 Firefox/51.0");
		event.put(Fields._UUID.value(), UUID.randomUUID().toString());
		

		JSONObject test = new JSONObject();
		test.put("name", "klaus");
		event.put("test", test);

		event.put("age", 25);
		event.put("average", 25.5);
		event.put("event", "pageLoaded");
		
		JSONObject meta = new JSONObject();
		meta.put("ip", "88.153.198.210");
		
		final int COUNT = 10000;
		final long EXPECTED_SIZE = COUNT + instance.index().size();
		
		for (int i = 0; i < COUNT; i++) {
			event.put(Fields._UUID.value(), UUID.randomUUID().toString());
			instance.track(TestHelper.event(event, new JSONObject()));
		}
		
		System.out.println("size: " + instance.index().size());
		System.out.println("expected: " + EXPECTED_SIZE);
		
		Assertions.assertThat(instance.index().size()).isEqualTo(EXPECTED_SIZE);
		
		
		reopen();
		
		Assertions.assertThat(instance.index().size()).isEqualTo(EXPECTED_SIZE);
		
		Thread.sleep(10000);
		
		Assertions.assertThat(instance.index().size()).isEqualTo(EXPECTED_SIZE);
		
		reopen();
		
		Assertions.assertThat(instance.index().size()).isEqualTo(EXPECTED_SIZE);
	}
	
}
