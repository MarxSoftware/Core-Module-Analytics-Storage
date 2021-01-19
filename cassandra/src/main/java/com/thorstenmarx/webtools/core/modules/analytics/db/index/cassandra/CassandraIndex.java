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
package de.marx_software.webtools.core.modules.analytics.db.index.cassandra;

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
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.term.Term;
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
import java.util.UUID;
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
public class CassandraIndex implements Index {

	private static final Logger LOGGER = LoggerFactory.getLogger(CassandraIndex.class);

	public static final String DEFAULT_INDEX = "webtools-analytics";

	private static final Map<String, Object> DEFAULT_CONFIG = new HashMap<>();

	static {
		DEFAULT_CONFIG.put("index", DEFAULT_INDEX);
	}

	private final Configuration configuration;

	private CqlSession session;

	public CassandraIndex(final Configuration configuration, final CqlSession session) {
		Objects.requireNonNull(configuration);
		Objects.requireNonNull(session);
		this.configuration = configuration;
		this.session = session;
	}

	private String index() {
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

		JSONObject data = document.json;
		RegularInsert insertStatement = QueryBuilder.insertInto("analytics", "events")
//				.value("id", QueryBuilder.literal(data.getString(Fields._UUID.value())))
				.value("id", QueryBuilder.literal(UUID.randomUUID()))
				.value("type", QueryBuilder.literal(data.getString(Fields.Event.value())))
				.value("version", QueryBuilder.literal(data.getIntValue(Fields.VERSION.value())))
				.value("site", QueryBuilder.literal(data.getString(Fields.Site.value())))
				.value("timestamp", QueryBuilder.literal(data.getLong(Fields._TimeStamp.value())))
				.value("source", QueryBuilder.literal(document.json.toJSONString()));

		ResultSet insertionResult = session.execute(insertStatement.build());
	}

	@Override
	public List<ShardDocument> search(Query query) {
		return Collections.EMPTY_LIST;
	}

	@Override
	public long size() {
		SimpleStatement countStatment = QueryBuilder.selectFrom("analytics", "events").countAll().build();
		ResultSet result = session.execute(countStatment);
		return result.one().getLong(0);
	}

}
