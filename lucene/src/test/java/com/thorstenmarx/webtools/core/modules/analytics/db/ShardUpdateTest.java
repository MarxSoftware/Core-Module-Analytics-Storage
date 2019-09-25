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
import org.testng.annotations.Test;
import net.engio.mbassy.bus.MBassador;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

/**
 *
 * @author thmarx
 */
public class ShardUpdateTest {

	DefaultAnalyticsDb instance;
	MockedExecutor executor = new MockedExecutor();
	
	@BeforeClass(enabled = false)
	public void setup() {
		Configuration config = new Configuration("target/test_update");
		
		instance = new DefaultAnalyticsDb(config, new MBassador(), executor);

		instance.open();
	}

	@AfterClass(enabled = false)
	public void tearDown() throws InterruptedException, Exception {
		instance.close();
		executor.shutdown();
	}

	/**
	 * Test of open method, of class AnalyticsDb.
	 */
	@Test(enabled =  false)
	public void test_update() throws Exception {

		System.out.println("test update");

		
	}

}
