package com.thorstenmarx.webtools.core.modules.analytics.db.index.lucene;

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
import com.thorstenmarx.webtools.api.analytics.Fields;
import com.thorstenmarx.webtools.api.analytics.query.Query;

import com.thorstenmarx.webtools.api.analytics.query.ShardDocument;
import com.thorstenmarx.webtools.core.modules.analytics.db.Configuration;
import com.thorstenmarx.webtools.core.modules.analytics.db.DefaultAnalyticsDb;
import com.thorstenmarx.webtools.core.modules.analytics.db.index.Index;
import com.thorstenmarx.webtools.core.modules.analytics.db.index.IndexDocument;
import com.thorstenmarx.webtools.core.modules.analytics.db.index.lucene.selection.HashShardSelectionStrategy;
import com.thorstenmarx.webtools.core.modules.analytics.db.index.lucene.selection.RoundRobinShardSelectionStrategy;
import com.thorstenmarx.webtools.core.modules.analytics.db.index.lucene.shard.LuceneShard;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.lucene.store.AlreadyClosedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author marx
 */
public class LuceneIndex implements Index, AutoCloseable {

	private static final Logger LOGGER = LoggerFactory.getLogger(LuceneIndex.class);

	private final Configuration configuration;
	/**
	 * List of all shards, normaly only the newest is writable
	 */
	private final List<LuceneShard> shards = new ArrayList<>();

	public static final int DEFAULT_SHARD_COUNT = 3;
	private int shardCount = DEFAULT_SHARD_COUNT;

	private String indexPath;

	private final Properties indexConfiguration = new Properties();

	final DefaultAnalyticsDb adb;

	private ShardSelectionStrategy<LuceneShard> shardSelectionStrategy;

	private boolean closed = false;

	/**
	 *
	 * @param configuration The Configuration.
	 * @param adb The database.
	 */
	public LuceneIndex(final Configuration configuration, final DefaultAnalyticsDb adb) {
		this.configuration = configuration;
		this.adb = adb;

		this.shardCount = configuration.shards;
//		this.shardSelectionStrategy = new RoundRobinShardSelectionStrategy(shards);
	}

	/**
	 *
	 * @return 
	 * @throws IOException If something goes wrong.
	 */
	@Override
	public Index open() throws IOException {
		indexPath = configuration.directory;

		if (!indexPath.endsWith("/")) {
			indexPath += "/";
		}
		indexPath += DefaultAnalyticsDb.ANALYTICS_DIR + "/index/";

		File config = new File(indexPath, "index.properties");
		if (config.exists()) {
			indexConfiguration.load(new FileReader(config));
		}

		File indexDir = new File(indexPath);
		if (!indexDir.exists()) {
			indexDir.mkdirs();
		}

		// load shards
		String[] availableShards = indexDir.list((File current, String name) -> new File(current, name).isDirectory());
		Arrays.sort(availableShards);
		// open shards
		int count = 0;
		for (String name : availableShards) {
			LuceneShard shard = new LuceneShard(name, configuration, adb);
			shard.open();
			shards.add(count, shard);
			count++;
		}
		while (count < shardCount) {
			createShard();
			count++;
		}
		
		this.shardSelectionStrategy = new HashShardSelectionStrategy<>(shards);

		closed = false;

		return this;
	}

	private int getNextShardID() throws IOException {
		int index = Integer.parseInt(indexConfiguration.getProperty("shard.id", "0"));
		index++;

		Map<String, String> properties = new HashMap<>();
		properties.put("shard.id", String.valueOf(index));
		setPropertiesAndSave(properties);

		return index;
	}

	private void setPropertiesAndSave(final Map<String, String> properties) throws IOException {
		properties.entrySet().forEach((entry) -> {
			indexConfiguration.setProperty(entry.getKey(), entry.getValue());
		});

		File indexProperties = new File(indexPath + "index.properties");
		indexConfiguration.store(new FileWriter(indexProperties), "update properties");
	}

	/**
	 *
	 */
	public void reopen() {
		shards.forEach((s) -> {
			s.reopen();
		});
	}

	/**
	 *
	 * @throws IOException
	 */
	private void createShard() throws IOException {
		String name = String.format("shard_%010d", getNextShardID());
		new File(indexPath + name).mkdir();
		LuceneShard s = new LuceneShard(name, configuration, this.adb);
		s.open();
		shards.add(s);
	}

	/**
	 *
	 * @throws IOException If something goes wrong.
	 */
	@Override
	public void close() throws IOException {
		if (!closed) {
			shards.forEach((shard) -> {
				try {
					shard.close();
				} catch (IOException ex) {
					LOGGER.error("error closing shard " + shard.getName(), ex);
				} catch (AlreadyClosedException e) {
					LOGGER.warn("shard storage closed already", e);
				}
			});
		}
		closed = true;
	}

	/**
	 * A simple bulk import.
	 *
	 * @param document The Document to add.
	 * @throws IOException If something goes wrong.
	 */
	@Override
	public void add(final IndexDocument document) throws IOException {
		if (document == null) {
			return;
		}
		try {
			final LuceneShard shard = selectShard(document);
			shard.addToLog(document);
		} catch (IOException e) {
			LOGGER.error("", e);
			throw e;
		}
	}

	private LuceneShard selectShard(final IndexDocument document) {
		return shardSelectionStrategy.route(document.json.getString(Fields._UUID.value()));
	}

	protected List<LuceneShard> getShards() {
		return shards;
	}

	/**
	 * Execute search.
	 *
	 * @param query The query for the search.
	 * @return List of ShardDocument.
	 */
	@Override
	public List<ShardDocument> search(Query query) {

		List<ShardDocument> result = new java.util.concurrent.CopyOnWriteArrayList<>();
		shards.stream().filter((shard) -> (shard.hasData(query.start(), query.end()))).forEach((shard) -> {
			try {
				final List<ShardDocument> shardResult = shard.search(query);
				result.addAll(shardResult);
			} catch (IOException ex) {
				LOGGER.error("", ex);
			}
		});

		return Collections.unmodifiableList(result);
	}

	@Override
	public long size() {
		long size = 0;
		for (LuceneShard shard : shards) {
			size += shard.size();
		}
		return size;
	}
}
