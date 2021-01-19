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
package de.marx_software.webtools.core.modules.analytics.db.index.elastic;

/*-
 * #%L
 * webtools-analytics-elastic
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

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import de.marx_software.webtools.api.analytics.Fields;
import de.marx_software.webtools.api.analytics.query.Query;
import de.marx_software.webtools.api.analytics.query.ShardDocument;
import de.marx_software.webtools.core.modules.analytics.db.Configuration;
import de.marx_software.webtools.core.modules.analytics.db.index.Index;
import de.marx_software.webtools.core.modules.analytics.db.index.IndexDocument;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Neben firewall Regeln zur Absicherung durch unbefugten Zugriff sollte auch
 * Basic Authentication via nginx gemacht werden:
 * https://www.elastic.co/de/blog/playing-http-tricks-nginx
 *
 * @author marx
 */
public class ElasticIndex implements Index {

	private static final Logger LOGGER = LoggerFactory.getLogger(ElasticIndex.class);
	
	public static final String SOURCE_FIELD = "#" + Fields.SOURCE.value();
	public static final String TIMESTAMP_FIELD = "#" + Fields._TimeStamp.value();
	public static final String VERSION_FIELD = "#" + Fields.VERSION.value();

	public static final String DEFAULT_INDEX = "webtools-analytics";
	
	private static final Map<String, Object> DEFAULT_CONFIG = new HashMap<>();
	static {
		DEFAULT_CONFIG.put("index", DEFAULT_INDEX);
	}

	private final Configuration configuration;

	private RestHighLevelClient client;

	public ElasticIndex(final Configuration configuration, final RestHighLevelClient elastic) {
		Objects.requireNonNull(configuration);
		Objects.requireNonNull(elastic);
		this.configuration = configuration;
		this.client = elastic;
	}
	
	private String index () {
		return configuration.index;
	}

	@Override
	public Index open() throws IOException {
		return this;
	}

	@Override
	public void close() throws Exception {
		
	}

	@Override
	public void add(IndexDocument document) throws IOException {
		final Map<String, Object> attributes = new HashMap<>();

		if (!document.json.containsKey(Fields._TimeStamp.value())) {
			document.json.put(Fields._TimeStamp.value(), System.currentTimeMillis());
		}
		document.json.put(SOURCE_FIELD, document.json.toJSONString());
		flatJsonObject(null, document.json, attributes);

		IndexRequest indexRequest = new IndexRequest(index())
				.source(attributes);
//		indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
		
		IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
		System.out.println(response.getResult());
	}

	@Override
	public List<ShardDocument> search(Query query) {
		BoolQueryBuilder boolQuery = new BoolQueryBuilder();
		boolQuery.filter(QueryBuilders.rangeQuery((TIMESTAMP_FIELD))
				.from(query.start())
				.to(query.end())
				.includeLower(true)
				.includeUpper(true));

		if (query.terms() != null && !query.terms().isEmpty()) {
			query.terms().entrySet().forEach((e) -> {
				boolQuery.filter(QueryBuilders.termQuery(e.getKey() + "_na.keyword", e.getValue()));
			});
		}

		if (query.multivalueTerms() != null && !query.multivalueTerms().isEmpty()) {
			BoolQueryBuilder multiBoolBuilder = new BoolQueryBuilder();
			query.multivalueTerms().entrySet().forEach(entry -> {
				for (final String value : entry.getValue()) {
					multiBoolBuilder.should(QueryBuilders.termQuery(entry.getKey() + "_na.keyword", value));
				}
			});
			boolQuery.filter(multiBoolBuilder);
		}

		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query(boolQuery);

		SearchRequest searchRequest = new SearchRequest(index());
		searchRequest.source(searchSourceBuilder);

		SearchResponse response;
		try {
			response = client.search(searchRequest, RequestOptions.DEFAULT);

			List<ShardDocument> result = new ArrayList<>();
			for (final SearchHit hit : response.getHits().getHits()) {
				final String sourceContent = (String) hit.getSourceAsString();

				result.add(new ShardDocument("elastic", JSONObject.parseObject(sourceContent)));
			}

			return result;
		} catch (IOException ex) {
			LOGGER.error("", ex);
		}

		return Collections.EMPTY_LIST;
	}

	@Override
	public long size() {
		try {
			CountRequest countRequest = new CountRequest(index());
			CountResponse response = client.count(countRequest, RequestOptions.DEFAULT);

			return response.getCount();
		} catch (IOException ex) {
			LOGGER.error("", ex);
		}
		
		return 0;
	}

	private void flatJsonObject(final String name, final JSONObject json, final Map<String, Object> doc) {
		json.keySet().stream().forEach((key) -> {
			// The _source field is already used in elasticsearch
			String tempKey = key;
			if (key.equals(Fields.SOURCE.value())) {
				tempKey = SOURCE_FIELD;
			} else if (key.equals(Fields._TimeStamp.value())) {
				tempKey = TIMESTAMP_FIELD;
			} else if (key.equals(Fields.VERSION.value())) {
				tempKey = VERSION_FIELD;
			}
			String localname = name != null ? (name + ".") : "";
			localname += tempKey;
			Object value = json.get(key);
			if (value instanceof JSONArray) {
				JSONArray array = (JSONArray) value;
				flatJsonArray(localname, array, doc);
			} else if (value instanceof JSONObject) {
				flatJsonObject(localname, (JSONObject) value, doc);
			} else {
				handleItem(localname, value, doc);
			}
		});
	}

	private void flatJsonArray(final String name, final JSONArray array, final Map<String, Object> doc) {
		array.stream().forEach((item) -> {
			if (item instanceof JSONArray) {
				flatJsonArray(name, (JSONArray) item, doc);
			} else if (item instanceof JSONObject) {
				flatJsonObject(name, (JSONObject) item, doc);
			} else {
				handleItem(name, item, doc);
			}
		});
	}

	private void handleItem(final String name, final Object value, final Map<String, Object> doc) {
		addValue(name, value, doc);
	}

	private void addValue(String key, Object value, Map<String, Object> doc) {
		if (value instanceof Integer) {
			doc.put(key, (Integer) value);
		} else if (value instanceof Long) {
			doc.put(key, (Long) value);
		} else if (value instanceof Float) {
			doc.put(key, (Float) value);
		} else if (value instanceof Double) {
			doc.put(key, (Double) value);
		} else if (value instanceof String) {
			doc.put(key + "_na", (String) value);
		} else if (value instanceof Boolean) {
			doc.put(key, value);
		}
	}

}
