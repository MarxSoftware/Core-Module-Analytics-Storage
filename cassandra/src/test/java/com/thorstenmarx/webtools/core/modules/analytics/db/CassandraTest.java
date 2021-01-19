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
package de.marx_software.webtools.core.modules.analytics.db;

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
import com.datastax.oss.driver.api.core.CqlSession;
import de.marx_software.webtools.api.analytics.Fields;
import java.io.IOException;
import java.util.UUID;
import net.engio.mbassy.bus.MBassador;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 *
 * @author marx
 */
public class CassandraTest {

	CqlSession session;
	private String indexName;

	@BeforeClass
	public void setup() throws IOException {
		session = CqlSession.builder().build();
	}

	@AfterClass
	public void tearDown() throws IOException {
		session.close();
	}

	/**
	 * Test of open method, of class AnalyticsDb.
	 */
	@Test(enabled = true)
	public void testAnalyticsDb() throws Exception {

		System.out.println("running analytics db test");

		Configuration config = new Configuration(indexName, "localhost:9200");

		try (CassandraAnalyticsDb instance = new CassandraAnalyticsDb(config, new MBassador(), session)) {
			instance.open();

			System.out.println(instance.index().size());
			long before = System.currentTimeMillis();
			for (int i = 0; i < 100; i++) {
				JSONObject data = new JSONObject();

				long timestamp = System.currentTimeMillis();
				data.put(Fields._TimeStamp.value(), timestamp);
				data.put("ua", "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:51.0) Gecko/20100101 Firefox/51.0");
				data.put(Fields._UUID.value(), UUID.randomUUID().toString());
				data.put("event", "pageview");
				data.put("site", "atest_site");
				data.put("version", 1);

				JSONObject meta = new JSONObject();
				meta.put("ip", "88.153.198.210");

				instance.track(TestHelper.event(data, meta));
			}

			long after = System.currentTimeMillis();
			System.out.println((after - before) + " ms");
			System.out.println(instance.index().size());
		}
	}
}
