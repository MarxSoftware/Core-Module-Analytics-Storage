package com.thorstenmarx.webtools.analytics.db.index.lucene.translog;

/*-
 * #%L
 * webtools-analytics
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


import com.thorstenmarx.webtools.api.analytics.Searchable;
import com.thorstenmarx.webtools.api.analytics.query.Query;
import com.thorstenmarx.webtools.api.analytics.query.ShardDocument;
import com.thorstenmarx.webtools.api.execution.Executor;
import com.thorstenmarx.webtools.core.modules.analytics.db.Configuration;
import com.thorstenmarx.webtools.core.modules.analytics.db.MockedExecutor;
import com.thorstenmarx.webtools.core.modules.analytics.db.index.IndexDocument;
import com.thorstenmarx.webtools.core.modules.analytics.db.index.lucene.Shard;
import com.thorstenmarx.webtools.core.modules.analytics.db.index.lucene.translog.LevelDBTransLog;
import com.thorstenmarx.webtools.core.modules.analytics.db.index.lucene.TransLog;
import com.thorstenmarx.webtools.core.modules.analytics.db.index.lucene.translog.TransLogTest;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 *
 * @author marx
 */
public class LevelDBTransLogTest extends TransLogTest {
	
	private LevelDBTransLog translog;
	private MockShard shard;
	private Executor executor = new MockedExecutor();

	@BeforeMethod
	public void setUpClass() throws Exception {
		Configuration config = new Configuration("target/translog-test-" + System.currentTimeMillis());
		
		shard = new MockShard();
		translog = new LevelDBTransLog(config, shard, executor);
		translog.open();
	}

	@AfterMethod
	public void tearDownClass() throws Exception {
		if (translog != null) {
			translog.close();
		}
	}
	
	@Override
	public TransLog translog() {
		return translog;
	}
	
	public Shard shard () {
		return shard;
	}

	@Override
	public TransLog translog(final Configuration config) {
		return new LevelDBTransLog(config, new MockShard(), executor);
	}
	
	
	
	@Test(invocationCount = 10)
	public void test_commit_to_shard () throws IOException {
		System.out.println("SIZE: " + translog.size());
		for (int i = 0; i < translog.maxSize(); i++) {
			translog().append(createDoc("horst " + i));
		}
		System.out.println("SIZE: " + translog.size());
		Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> shard.addCount.get() == translog.maxSize());
		Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> shard.reopenCount.get() == 1);
	}

	
	private static class MockShard implements Shard, Searchable {

		AtomicInteger reopenCount = new AtomicInteger(0);
		AtomicInteger addCount = new AtomicInteger(0);
		public MockShard() {
		}

		@Override
		public void add(IndexDocument indexDocument) throws IOException {
			addCount.incrementAndGet();
		}

		@Override
		public void reopen() {
			reopenCount.incrementAndGet();
		}

		@Override
		public String getName() {
			return "memory";
		}

		@Override
		public List<ShardDocument> search(Query query) throws IOException {
			return Collections.EMPTY_LIST;
		}

		@Override
		public boolean hasData(long from, long to) {
			return true;
		}

		@Override
		public int size() {
			return addCount.get();
		}

		@Override
		public boolean isLocked() {
			return false;
		}
	}
	
}
