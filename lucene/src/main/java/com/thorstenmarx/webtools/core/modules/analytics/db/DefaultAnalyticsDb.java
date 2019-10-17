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
import com.thorstenmarx.webtools.api.analytics.query.Aggregator;
import com.thorstenmarx.webtools.api.analytics.query.LimitProvider;
import com.thorstenmarx.webtools.api.analytics.query.Query;
import com.thorstenmarx.webtools.api.analytics.query.ShardDocument;
import com.thorstenmarx.webtools.api.analytics.query.ShardedQuery;
import com.thorstenmarx.webtools.api.execution.Executor;
import com.thorstenmarx.webtools.core.modules.analytics.db.index.lucene.AsyncShardQuery;
import com.thorstenmarx.webtools.core.modules.analytics.db.index.lucene.LuceneIndex;
import com.thorstenmarx.webtools.core.modules.analytics.pipeline.DBUpdateStage;
import com.thorstenmarx.webtools.core.modules.analytics.pipeline.EventContext;
import com.thorstenmarx.webtools.core.modules.analytics.util.pipeline.Pipeline;
import com.thorstenmarx.webtools.core.modules.analytics.util.pipeline.SequentialPipeline;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Struktur: &lt;user&gt; (1)-&gt;(N) &lt;visit&gt; (1)-&gt;(N) &lt;request&gt;
 * (1)-&gt;(N) &lt;event&gt;
 *
 * @author marx
 */
public class DefaultAnalyticsDb extends AbstractAnalyticsDb<LuceneIndex> {

	private static final Logger LOGGER = LoggerFactory.getLogger(DefaultAnalyticsDb.class);

	public static final String ANALYTICS_DIR = "analytics";

	private final Configuration configuration;
	/**
	 * Sharded lucene index
	 */
	private LuceneIndex index = null;

	private final Pipeline analyticsPipline;

	private boolean isClosing = false;
	
	private final Executor executor;

	public DefaultAnalyticsDb(final Configuration configuration, final Executor executor) {
		this.configuration = configuration;
		this.executor = executor;

		this.analyticsPipline = new SequentialPipeline();
		this.analyticsPipline.addStage(new DBUpdateStage());
	}
	
	public Executor getExecutor () {
		return executor;
	}

	public DefaultAnalyticsDb open()  {
		try {
			this.isClosing = false;
			String directory = (String) configuration.directory;
			if (!directory.endsWith("/")) {
				directory += "/";
			}
			directory += ANALYTICS_DIR + "/";
			// open index
			index = new LuceneIndex(configuration, this);
			index.open();
			
			// open fast lookup db
			File lookupsdir = new File(directory + "lookups");
			if (!lookupsdir.exists()) {
				lookupsdir.mkdirs();
			}
		} catch (IOException ex) {
			throw new IllegalStateException(ex);
		}
		
		return this;
	}

	@Override
	public void close() throws IOException, InterruptedException, Exception {
		this.isClosing = true;

		if (index != null) {
			index.close();
		}
	}

	public boolean isClosing() {
		return isClosing;
	}

	@Override
	public void track(Map<String, Map<String, Object>> event) {
		JSONObject json = new JSONObject();
		json.putAll(event);
		
		EventContext eventContext = new EventContext(json, this);
		getPipeline().execute(eventContext);
	}
	
	/**
	 * Internal method to check if a uuid is already indexed.
	 * @param uuid
	 * @return
	 * @throws InterruptedException
	 * @throws ExecutionException 
	 */
	public boolean exists (final String uuid) throws InterruptedException, ExecutionException {
		if (uuid == null) {
			return false;
		}
		Query query = Query.builder().term(Fields._UUID.value(), uuid).build();
		CompletableFuture<List<ShardDocument>> future = CompletableFuture.supplyAsync(() -> {
			return index.search(query);
		});
		
		return future.get().size() > 0;
	}

	@Override
	public <T> CompletableFuture<T> query(final Query query, final Aggregator<T> aggregator) {
		CompletableFuture<List<ShardDocument>> future = CompletableFuture.supplyAsync(() -> {
			return index.search(query);
		});

		CompletableFuture<T> future2 = future.thenApplyAsync((documents) -> {
			try {
				aggregator.documents(documents);
				return aggregator.call();
			} catch (Exception ex) {
				LOGGER.error("", ex);
			}
			return null;
		});

		return future2;

	}

	@Override
	public Pipeline getPipeline() {
		return analyticsPipline;
	}

	@Override
	public LuceneIndex index() {
		return index;
	}

	@Override
	public <R, S, Q extends LimitProvider> R queryAsync(ShardedQuery<R, S, Q> query) {
		return ForkJoinPool.commonPool().invoke(new AsyncShardQuery<>(this, query));
	}


}
