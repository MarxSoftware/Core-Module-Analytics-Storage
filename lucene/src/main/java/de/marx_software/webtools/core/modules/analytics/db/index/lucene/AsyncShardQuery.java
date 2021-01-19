package de.marx_software.webtools.core.modules.analytics.db.index.lucene;

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
import de.marx_software.webtools.api.analytics.query.LimitProvider;
import de.marx_software.webtools.api.analytics.query.ShardedQuery;
import de.marx_software.webtools.core.modules.analytics.db.DefaultAnalyticsDb;
import de.marx_software.webtools.core.modules.analytics.db.index.lucene.shard.LuceneShard;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RecursiveTask;
import java.util.stream.Collectors;

/**
 *
 * @author thmarx
 * @param <R>
 * @param <S>
 * @param <Q>
 */
public class AsyncShardQuery<R, S, Q extends LimitProvider> extends RecursiveTask<R> {

	private static final long serialVersionUID = 726397759360339054L;

	transient protected final DefaultAnalyticsDb db;
	transient protected final Q query;

	transient protected final ShardedQuery<R, S, Q> shardedQuery;

	/**
	 * The asynchrone query.
	 *
	 * @param db
	 * @param shardedQuery
	 */
	public AsyncShardQuery(final DefaultAnalyticsDb db, final ShardedQuery<R, S, Q> shardedQuery) {
		this.db = db;
		this.query = shardedQuery.query();
		this.shardedQuery = shardedQuery;
	}

	@Override
	protected R compute() {

		// select shards
		List<LuceneShard> targetShards = db.index().getShards().stream().filter((shard) -> (shard.hasData(query.start(), query.end()))).collect(Collectors.toList());

		List<RecursiveTask<S>> shardTasks = new ArrayList<>();
		targetShards.stream().map(target -> shardedQuery.getSubTask(query, target)).map(task -> {
			task.fork();
			return task;
		}).forEach((task) -> {
			shardTasks.add(task);
		});
		List<S> subResults = new ArrayList<>();
		shardTasks.stream().forEach(task -> subResults.add(task.join()));

		return shardedQuery.merge(subResults);
	}
}
