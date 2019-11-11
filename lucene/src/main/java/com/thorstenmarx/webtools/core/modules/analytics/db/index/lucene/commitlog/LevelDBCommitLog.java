package com.thorstenmarx.webtools.core.modules.analytics.db.index.lucene.commitlog;

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
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Charsets;
import com.thorstenmarx.webtools.api.analytics.Fields;
import com.thorstenmarx.webtools.api.analytics.Searchable;
import com.thorstenmarx.webtools.api.execution.Executor;
import com.thorstenmarx.webtools.core.modules.analytics.db.Configuration;
import com.thorstenmarx.webtools.core.modules.analytics.db.DefaultAnalyticsDb;
import com.thorstenmarx.webtools.core.modules.analytics.db.index.IndexDocument;
import com.thorstenmarx.webtools.core.modules.analytics.db.index.lucene.CommitLog;
import com.thorstenmarx.webtools.core.modules.analytics.db.index.lucene.Shard;
import static com.thorstenmarx.webtools.core.modules.analytics.util.FileUtils.addEndingSeparator;
import static com.thorstenmarx.webtools.core.modules.analytics.util.FileUtils.ensureExistence;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.iq80.leveldb.*;
import static org.iq80.leveldb.impl.Iq80DBFactory.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author marx
 */
public class LevelDBCommitLog implements CommitLog {

	private static final Logger LOGGER = LoggerFactory.getLogger(LevelDBCommitLog.class);

	public static final int DEFAULT_MAX_SIZE = 100;

	private final ReadWriteLock lock = new ReentrantReadWriteLock();

	private final Configuration config;
	private DB db;
	private MemoryShard memoryShard;

	private final Shard shard;

	private int maxSize = DEFAULT_MAX_SIZE;

	private final Executor executor;
	
	private static final AtomicInteger variableSize = new AtomicInteger(10);

	public LevelDBCommitLog(final Configuration config, final Shard shard, final Executor executor) {
		this.config = config;
		this.shard = shard;
		this.executor = executor;

		maxSize = config.translog_maxsize;
		maxSize += variableSize.getAndAdd(10);
	}

	@Override
	public Lock readLock() {
		return lock.readLock();
	}

	@Override
	public void append(final IndexDocument document) throws IOException {
		try {
			lock.writeLock().lock();

			memoryShard.add(document);
			byte[] documentBytes = document.json.toJSONString().getBytes(Charsets.UTF_8);
			final String uuid = document.json.getString(Fields._UUID.value());

			db.put(bytes(uuid), documentBytes);

			if (memoryShard.size() >= maxSize) {
				commit();
			}

		} finally {
			lock.writeLock().unlock();
		}
	}

	/**
	 *
	 */
	private void commit() {

		final Runnable commitRunnable = () -> {
			lock.writeLock().lock();
			try {
				memoryShard.getDocuments().forEach((uuid, document) -> {
					try {
						IndexDocument indexDoc = new IndexDocument(document.obj);
						shard.add(indexDoc);
						db.delete(bytes(uuid), new WriteOptions().sync(true));
					} catch (IOException ex) {
						LOGGER.error("Error commiting document to shard", ex);
					}
				});

				shard.reopen();
				memoryShard.clear();
			} finally {
				lock.writeLock().unlock();
			}

		};
//		commitRunnable.run();
		executor.execute(commitRunnable);

	}

	@Override
	public void flush() {
		commit();
	}
		
	@Override
	public void close() throws IOException {
		db.close();
		memoryShard.close();
	}

	@Override
	public void open() throws IOException {

		LOGGER.info("create translog with maxSize " + maxSize);

		this.memoryShard = new MemoryShard();

		String dataDir = config.directory;
		dataDir = addEndingSeparator(dataDir);
		dataDir += DefaultAnalyticsDb.ANALYTICS_DIR + "/index/" + shard.getName() + "/commitlog/";
		ensureExistence(dataDir);

		File dataFile = new File(dataDir);
		Options options = new Options();
		options.createIfMissing(true);
		db = factory.open(dataFile, options);
		if (db.iterator().hasNext()) {
			recovery();
		}
	}

	@Override
	public int size() {
		lock.readLock().lock();
		try {
			return memoryShard.size();
		} finally {
			lock.readLock().unlock();
		}
	}

	private void recovery() throws IOException {
		LOGGER.info("found uncommited changes");
		LOGGER.info("start recovery");
		DBIterator it = db.iterator();
		int count = 0;
		while (it.hasNext()) {
			Map.Entry<byte[], byte[]> next = it.next();
			final String uuid = asString(next.getKey());
			final String content = asString(next.getValue());

			JSONObject jsonObject = JSONObject.parseObject(content);
			IndexDocument doc = new IndexDocument(jsonObject);
			append(doc);

			count++;
		}
		LOGGER.info("recovery of %d uncommited changes finished", count);
	}

	@Override
	public Searchable getSearchable() {
		lock.readLock().lock();
		try {
			return memoryShard;
		} finally {
			lock.readLock().unlock();
		}
	}

	@Override
	public boolean isLocked() {
		if (lock.writeLock().tryLock()) {
			try {
				return true;
			} finally {
				lock.writeLock().unlock();
			}
		} else {
			return false;
		}
	}

	@Override
	public int maxSize() {
		return maxSize;
	}

}
