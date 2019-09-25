/**
 * WebTools-Platform
 * Copyright (C) 2016-2018  ThorstenMarx (kontakt@thorstenmarx.com)
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
import com.thorstenmarx.webtools.api.analytics.Fields;
import java.util.UUID;
import net.engio.mbassy.bus.MBassador;
import org.testng.annotations.Test;

/**
 *
 * @author thmarx
 */
public class LargeAnalyticsDbTest {

	/**
	 * Test of open method, of class AnalyticsDb.
	 */
	@Test(enabled = false)
	public void testOpenAndClose() throws Exception {
		
		System.out.println("running large analytics db test");

		Configuration config = new Configuration("target/adb-" + System.currentTimeMillis());
		

		MockedExecutor executor = new MockedExecutor();
		DefaultAnalyticsDb instance = new DefaultAnalyticsDb(config, new MBassador(), executor);
		instance.open();

		for (int i = 0; i < 100000; i++) {
			long timestamp = System.currentTimeMillis();
			String site = UUID.randomUUID().toString();
			String user = UUID.randomUUID().toString();
			String visit = UUID.randomUUID().toString();
			String request = UUID.randomUUID().toString();

			JSONObject event = new JSONObject();
			event.put("_siteid", site);
			event.put("_event", "pageLoaded");
			event.put("_userid", user);
			event.put("_visitid", visit);
			event.put("_reqid", request);
			event.put(Fields._TimeStamp.value(), timestamp);
			event.put("_source", "http://google.de");

			instance.track(TestHelper.event(event, new JSONObject()));
			
			if (i % 1000 == 0) {
				System.out.println("tracked = " + i);
			}
		}

		instance.close();
		executor.shutdown();
	}

}
