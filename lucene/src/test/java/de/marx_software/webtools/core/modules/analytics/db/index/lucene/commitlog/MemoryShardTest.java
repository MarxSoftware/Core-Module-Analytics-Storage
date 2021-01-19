package de.marx_software.webtools.core.modules.analytics.db.index.lucene.commitlog;

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
import de.marx_software.webtools.api.analytics.Fields;
import de.marx_software.webtools.api.analytics.query.Query;
import de.marx_software.webtools.api.analytics.query.ShardDocument;
import de.marx_software.webtools.core.modules.analytics.db.index.IndexDocument;
import de.marx_software.webtools.core.modules.analytics.pipeline.EventContext;
import java.io.IOException;
import java.util.List;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.IndexSearcher;
import org.assertj.core.api.Assertions;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author marx
 */
public class MemoryShardTest {

	@Test
	public void testRemove() throws IOException {
		MemoryShard shard = new MemoryShard();

		JSONObject json = new JSONObject();
		json.put(EventContext.KEY_TIMESTAMP, System.currentTimeMillis());
		json.put(Fields._UUID.value(), "auuid");
		IndexDocument doc = new IndexDocument(json);

		shard.add(doc);

		Assertions.assertThat(shard.size()).isEqualTo(1);

		shard.remove(json);

		Assertions.assertThat(shard.size()).isEqualTo(0);
	}

	@Test
	public void testQuery_Range() throws IOException {
		MemoryShard shard = new MemoryShard();

		long timestamp = System.currentTimeMillis();
		JSONObject json = new JSONObject();
		json.put(EventContext.KEY_TIMESTAMP, timestamp);
		json.put(Fields._UUID.value(), "auuid");
		IndexDocument doc = new IndexDocument(json);

		shard.add(doc);

		Query query = Query.builder().start(timestamp - 100).end(timestamp - 90).build();
		List<ShardDocument> result = shard.search(query);
		Assertions.assertThat(result).hasSize(0);

		query = Query.builder().start(timestamp + 100).end(timestamp + 190).build();
		result = shard.search(query);
		Assertions.assertThat(result).hasSize(0);

		query = Query.builder().start(timestamp - 1000).end(timestamp + 1000).build();
		result = shard.search(query);
		Assertions.assertThat(result).hasSize(1);
	}

	@Test
	public void testQuery_Event() throws IOException {
		MemoryShard shard = new MemoryShard();

		long timestamp = System.currentTimeMillis();
		JSONObject json = new JSONObject();
		json.put(EventContext.KEY_TIMESTAMP, timestamp);
		json.put(Fields._UUID.value(), "auuid");
		json.put(Fields.Event.value(), "click");
		IndexDocument doc = new IndexDocument(json);

		shard.add(doc);

		Query query = Query.builder().term(Fields.Event.value(), "impressions")
				.start(timestamp - 1000).end(timestamp + 1000).build();
		List<ShardDocument> result = shard.search(query);
		Assertions.assertThat(result).hasSize(0);

		query = Query.builder().term(Fields.Event.value(), "click")
				.start(timestamp - 1000).end(timestamp + 1000).build();
		result = shard.search(query);
		Assertions.assertThat(result).hasSize(1);
	}

	@Test
	public void testMemoryIndex() throws IOException {
		Document doc = new Document();
		doc.add(new LongPoint("longs", 100L));

		MemoryIndex mi = MemoryIndex.fromDocument(doc, new KeywordAnalyzer());
		IndexSearcher s = mi.createSearcher();

		Assert.assertEquals(s.count(LongPoint.newRangeQuery("longs", new long[]{10L}, new long[]{110L})), 1);
	}
}
