package com.thorstenmarx.webtools.core.modules.analytics.db.index.lucene.shard;

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

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.thorstenmarx.webtools.api.analytics.Fields;
import com.thorstenmarx.webtools.core.modules.analytics.pipeline.EventContext;

import java.io.IOException;
import java.util.Map;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;

/**
 *
 * @author marx
 */
public class DocumentBuilder {

	protected final Analyzer analyzer = new KeywordAnalyzer();
	
	public Document build(final JSONObject json) throws IOException {
		Document document = new Document();
		
		long timestamp = System.currentTimeMillis();
		if (json.containsKey(Fields._TimeStamp.value())) {
			timestamp = json.getLongValue(Fields._TimeStamp.value());
		}
		document.add(new NumericDocValuesField(Fields._TimeStamp.value(), timestamp));
		document.add(new LongPoint(Fields.TIMESTAMP_SORT.value(), timestamp));

		flatJsonObject(null, json, document);


		//document.add(new StoredField(Fields.SOURCE.value(), Snappy.compress(json.toJSONString())));


		return document;
	}

	protected void flatJsonObject(final String name, final JSONObject json, final Document doc) {
		json.keySet().stream().forEach((key) -> {
			String localname = name != null ? (name + ".") : "";
			localname += key;
			Object value = json.get(key);
			if (value instanceof JSONArray) {
				JSONArray array = (JSONArray) value;
				flatJsonArray(localname, array, doc);
			} else if (value instanceof JSONObject) {
				flatJsonObject(localname, (JSONObject) value, doc);
			} else if (value instanceof Map) {
				flatJsonObject(localname, new JSONObject((Map<String, Object>) value), doc);
			} else {
				handleItem(localname, value, doc);
			}
		});
	}

	private void flatJsonArray(final String name, final JSONArray array, final Document doc) {
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

	private void handleItem(final String name, final Object value, final Document doc) {
		addValue(name, value, doc);
	}

	private void addValue(String key, Object value, Document doc) {
		if (value instanceof Integer) {
			doc.add(new IntPoint(key, (Integer) value));
		} else if (value instanceof Long) {
			doc.add(new LongPoint(key, (Long) value));
		} else if (value instanceof Float) {
			doc.add(new FloatPoint(key, (Float) value));
		} else if (value instanceof Double) {
			doc.add(new DoublePoint(key, (Double) value));
		} else if (value instanceof String) {
			doc.add(new StringField(key, (String) value, Field.Store.YES));
		} else if (value instanceof Boolean) {
			doc.add(new StringField(key, String.valueOf(value), Field.Store.YES));
		}
	}
}
