package de.marx_software.webtools.core.modules.analytics.db.index.lucene.commitlog;

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


import de.marx_software.webtools.api.analytics.Searchable;
import de.marx_software.webtools.api.analytics.query.Query;
import de.marx_software.webtools.api.analytics.query.ShardDocument;
import de.marx_software.webtools.api.execution.Executor;
import de.marx_software.webtools.core.modules.analytics.db.Configuration;
import de.marx_software.webtools.core.modules.analytics.db.MockedExecutor;
import de.marx_software.webtools.core.modules.analytics.db.TestHelper;
import de.marx_software.webtools.core.modules.analytics.db.index.IndexDocument;
import de.marx_software.webtools.core.modules.analytics.db.index.lucene.Shard;
import de.marx_software.webtools.core.modules.analytics.db.index.lucene.commitlog.LevelDBCommitLog;
import de.marx_software.webtools.core.modules.analytics.db.index.lucene.CommitLog;
import de.marx_software.webtools.core.modules.analytics.db.index.lucene.commitlog.CommitLogTest;
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
public class LevelDBCommitLogTest extends CommitLogTest {
	
	private LevelDBCommitLog commitlog;
	private MockShard shard;
	private Executor executor = new MockedExecutor();

	@BeforeMethod
	public void setUpClass() throws Exception {
		Configuration config = TestHelper.getConfiguration("target/translog-test-" + System.currentTimeMillis());
		
		shard = new MockShard();
		commitlog = new LevelDBCommitLog(config, shard, executor);
		commitlog.open();
	}

	@AfterMethod
	public void tearDownClass() throws Exception {
		if (commitlog != null) {
			commitlog.close();
		}
	}
	
	@Override
	public CommitLog commitlog() {
		return commitlog;
	}
	
	public Shard shard () {
		return shard;
	}

	@Override
	public CommitLog commitlog(final Configuration config) {
		return new LevelDBCommitLog(config, new MockShard(), executor);
	}
	
	
	
	@Test(invocationCount = 10)
	public void test_commit_to_shard () throws IOException {
		System.out.println("SIZE: " + commitlog.size());
		for (int i = 0; i < commitlog.maxSize(); i++) {
			commitlog().append(createDoc("horst " + i));
		}
		System.out.println("SIZE: " + commitlog.size());
		Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> shard.addCount.get() == commitlog.maxSize());
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
