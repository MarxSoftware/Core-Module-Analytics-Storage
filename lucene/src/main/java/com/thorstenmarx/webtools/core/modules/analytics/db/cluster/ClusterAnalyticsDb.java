/*
 * Copyright (C) 2019 Thorsten Marx
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
package com.thorstenmarx.webtools.core.modules.analytics.db.cluster;

/*-
 * #%L
 * webtools-manager
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
import com.google.gson.Gson;
import com.thorstenmarx.webtools.api.analytics.AnalyticsDB;
import com.thorstenmarx.webtools.api.analytics.Filter;
import com.thorstenmarx.webtools.api.analytics.query.Aggregator;
import com.thorstenmarx.webtools.api.analytics.query.LimitProvider;
import com.thorstenmarx.webtools.api.analytics.query.Query;
import com.thorstenmarx.webtools.api.analytics.query.ShardedQuery;
import com.thorstenmarx.webtools.api.cluster.Cluster;
import com.thorstenmarx.webtools.api.cluster.Message;
import com.thorstenmarx.webtools.api.cluster.services.MessageService;
import com.thorstenmarx.webtools.core.modules.analytics.db.DefaultAnalyticsDb;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author marx
 */
public class ClusterAnalyticsDb implements AnalyticsDB, MessageService.MessageListener {

	private static final Logger LOGGER = LoggerFactory.getLogger(ClusterAnalyticsDb.class);
	
	private static final String EVENT_TRACK = "event_track";
	
	private final DefaultAnalyticsDb db;
	private final Cluster cluster;
	Gson gson = new Gson();

	public ClusterAnalyticsDb(final DefaultAnalyticsDb db, final Cluster cluster) {
		this.db = db;
		this.cluster = cluster;
		
		this.cluster.getMessageService().registerMessageListener(this);
	}
	
	public void close () {
		this.cluster.getMessageService().unregisterMessageListener(this);
	}

	@Override
	public <T> CompletableFuture<T> query(Query query, Aggregator<T> aggregator) {
		return db.query(query, aggregator);
	}

	@Override
	public <R, S, Q extends LimitProvider> R queryAsync(ShardedQuery<R, S, Q> query) {
		return db.queryAsync(query);
	}

	@Override
	public void track(final Map<String, Map<String, Object>> event) {
		try {
			PayloadTrack payload = new PayloadTrack();
			payload.event = event;
			
			Message message = new Message().setType(EVENT_TRACK).setPayload(gson.toJson(payload));
			cluster.getMessageService().publish(message);
		} catch (IOException ex) {
			LOGGER.error("", ex);
		}
	}

	@Override
	public void addFilter(Filter filter) {
		db.addFilter(filter);
	}

	@Override
	public List<Filter> getFilters() {
		return db.getFilters();
	}

	@Override
	public boolean hasFilters() {
		return db.hasFilters();
	}

	@Override
	public void handle(final Message message) {
		if (EVENT_TRACK.equals(message.getType())) {
			PayloadTrack payload = gson.fromJson(message.getPayload(), PayloadTrack.class);
			db.track(payload.event);
		}
	}
	
	public static class PayloadTrack {
		public Map<String, Map<String, Object>> event;
	}
	
}