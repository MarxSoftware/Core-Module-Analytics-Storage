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
package com.thorstenmarx.webtools.analytics.db;

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

import com.thorstenmarx.webtools.api.execution.Executor;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author marx
 */
public class MockedExecutor implements Executor {

	ScheduledExecutorService scheduler;
	public MockedExecutor () {
		scheduler = Executors.newSingleThreadScheduledExecutor();
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				scheduler.shutdownNow();
			}
			
		});
	}
	
	public void shutdown () {
		scheduler.shutdownNow();
	}
	
	@Override
	public CompletableFuture<Void> execute(Runnable task) {
		return CompletableFuture.runAsync(task, scheduler);
	}

	

	@Override
	public ScheduledFuture<?> schedule(Runnable task, long delay, TimeUnit timeUnit) {
		return scheduler.schedule(task, delay, timeUnit);
	}

	@Override
	public ScheduledFuture<?> scheduleFixedDelay(Runnable task, long initialDelay, long delay, TimeUnit timeUnit) {
		return scheduler.scheduleWithFixedDelay(task, initialDelay, delay, timeUnit);
	}

	@Override
	public ScheduledFuture<?> scheduleFixedRate(Runnable task, long initialDelay, long period, TimeUnit timeUnit) {
		return scheduler.scheduleAtFixedRate(task, initialDelay, period, timeUnit);
	}
	
}
