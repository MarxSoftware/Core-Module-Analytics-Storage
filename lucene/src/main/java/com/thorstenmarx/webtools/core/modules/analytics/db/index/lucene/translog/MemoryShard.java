
package com.thorstenmarx.webtools.core.modules.analytics.db.index.lucene.translog;

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
import com.thorstenmarx.webtools.api.analytics.Searchable;
import com.thorstenmarx.webtools.api.analytics.query.Query;
import com.thorstenmarx.webtools.api.analytics.query.ShardDocument;
import com.thorstenmarx.webtools.core.modules.analytics.db.index.IndexDocument;
import com.thorstenmarx.webtools.core.modules.analytics.db.index.lucene.shard.LuceneShard;
import com.thorstenmarx.webtools.core.modules.analytics.pipeline.EventContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.Nonnull;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.TermQuery;

/**
 *
 * @author marx
 */
public class MemoryShard implements Searchable {
	private final ConcurrentMap<String, MemoryDocument> documents;
	
	private final MemoryDocumentBuilder builder;

	static class MemoryDocument {
		protected final MemoryIndex index;
		protected final JSONObject obj;
		protected final String uuid;
		protected long timestamp;
	
		protected MemoryDocument (final MemoryIndex index, final JSONObject obj, final String uuid) {
			this.index = index;
			this.obj = obj;
			this.uuid = uuid;
			
			if (obj.containsKey(EventContext.KEY_TIMESTAMP)) {
				timestamp = obj.getLongValue(EventContext.KEY_TIMESTAMP);
			} else {
				timestamp = System.currentTimeMillis();
			}
			
		}
		
		public boolean inside (long from, long to) {
			return timestamp >= from && timestamp <= to;
		}
	}

	protected MemoryShard () {
		builder = new MemoryDocumentBuilder();
		documents = new ConcurrentHashMap<>();
	}
	
	public ConcurrentMap<String, MemoryDocument> getDocuments() {
		return new ConcurrentHashMap(documents);
	}
	
	public void add (final IndexDocument doc) throws IOException {
		final MemoryIndex mdoc = builder.buildMemory(doc.json);
		final String uuid = doc.json.getString(Fields._UUID.value());
		mdoc.freeze();
		documents.put(uuid, new MemoryDocument(mdoc, doc.json, uuid));
	}
	
	public void remove (@Nonnull final JSONObject json) {
		final String uuid = json.getString(Fields._UUID.value());
		remove(uuid);
	}

	public void remove(final String uuid) {
		documents.remove(uuid);
	}
	
	public void clear () {
		documents.forEach((uuid, doc) -> {
			builder.giveBack(doc.index);
		});
		documents.clear();
	}
	
	public void close () {
		clear();
	}
	
	@Override
	public boolean hasData(long from, long to) {
		return getDocuments().values().stream().anyMatch((doc) -> doc.inside(from, to));
	}
	
	@Override
	public List<ShardDocument> search (final Query query) throws IOException {
		BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();

		//NumericRangeQuery<Long> rangeQuery = NumericRangeQuery.newLongRange("timestamp_sort", query.start(), query.end(), true, true);
		org.apache.lucene.search.Query rangeQuery = LongPoint.newRangeQuery(Fields.TIMESTAMP_SORT.value(), query.start(), query.end());
		queryBuilder.add(rangeQuery, BooleanClause.Occur.MUST);

		if (query.terms() != null && !query.terms().isEmpty()) {
			query.terms().entrySet().forEach((e) -> {
				queryBuilder.add(new TermQuery(new Term(e.getKey(), e.getValue())), BooleanClause.Occur.MUST);
			});
		}

		if (query.multivalueTerms() != null && !query.multivalueTerms().isEmpty()) {
			query.multivalueTerms().entrySet().stream().map(LuceneShard::multivalueTermsToBooleanQuery).forEach(booleanQuery -> {
				queryBuilder.add(booleanQuery, BooleanClause.Occur.MUST);
			});
		}
		
		List<ShardDocument> result = new ArrayList<>();
		BooleanQuery booleanQuery = queryBuilder.build();
		documents.forEach((uuid, doc) -> {
			if (doc.index.search(booleanQuery) > 0f){
				result.add(new ShardDocument("memory shard", doc.obj));
			}
		});
		
		return result;
	}
	
	public int size () {
		return documents.size();
	}
}
