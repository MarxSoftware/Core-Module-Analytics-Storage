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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.store.AlreadyClosedException;
import org.testng.annotations.Test;

/**
 *
 * @author thmarx
 */
public class AlreadyClosedExceptionIssueTest {

	private static final long timestamp = System.nanoTime();
	
	AtomicInteger counter = new AtomicInteger();

	@Test(invocationCount = 20)
	public void openCloseTest() throws InterruptedException, Exception {
		int index = counter.incrementAndGet();
		Configuration config =  TestHelper.getConfiguration("target/adb-" + timestamp + "/" + index);

		MockedExecutor executor = new MockedExecutor();
		try (DefaultAnalyticsDb db = new DefaultAnalyticsDb(config, executor)) {
			db.open();
		} catch (AlreadyClosedException e) {
			System.out.println("exception on " + index);
		} finally {
			executor.shutdown();
		}
	}
}
