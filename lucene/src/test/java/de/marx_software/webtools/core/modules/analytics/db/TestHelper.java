package de.marx_software.webtools.core.modules.analytics.db;

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
import de.marx_software.webtools.core.modules.analytics.db.index.lucene.LuceneIndex;
import de.marx_software.webtools.core.modules.analytics.db.index.lucene.commitlog.LevelDBCommitLog;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author marx
 */
public class TestHelper {
	public static Map<String, Map<String, Object>> event (final JSONObject data, final JSONObject meta) {
		Map<String, Map<String, Object>> event = new HashMap<>();
		
		event.put("data", data);
		event.put("meta", meta);
		
		return event;
	}
	public static Map<String, Map<String, Object>> event (final Map<String, Object> data, final Map<String, Object> meta) {
		Map<String, Map<String, Object>> event = new HashMap<>();
		
		event.put("data", data);
		event.put("meta", meta);
		
		return event;
	}
	
	public static Configuration getConfiguration (final String path) {
		return new Configuration(path, LuceneIndex.DEFAULT_SHARD_COUNT, LevelDBCommitLog.DEFAULT_MAX_SIZE);
	}
}
