package com.thorstenmarx.webtools.core.modules.analytics.db.index.lucene.commitlog;

/*-
 * #%L
 * webtools-analytics
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
import com.thorstenmarx.webtools.core.modules.analytics.db.index.lucene.CommitLog;
import com.alibaba.fastjson.JSONObject;
import com.thorstenmarx.webtools.api.analytics.Fields;
import com.thorstenmarx.webtools.core.modules.analytics.db.Configuration;
import com.thorstenmarx.webtools.core.modules.analytics.db.TestHelper;
import com.thorstenmarx.webtools.core.modules.analytics.db.index.IndexDocument;
import com.thorstenmarx.webtools.core.modules.analytics.db.index.lucene.Shard;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 *
 * @author marx
 */
public abstract class CommitLogTest {

	public abstract CommitLog commitlog();

	public abstract CommitLog commitlog(Configuration config);
	
	public abstract Shard shard ();

	@Test
	public void test_append() throws IOException {
		IndexDocument doc = createDoc("eins");
		commitlog().append(doc);
		doc = createDoc("zwei");
		commitlog().append(doc);
	}

	@Test(invocationCount = 10)
	public void test_auto_reopen() throws IOException {
		Assertions.assertThat(commitlog().size()).isEqualTo(0);

		for (int i = 0; i <= commitlog().maxSize(); i++) {
			commitlog().append(createDoc("horst " + i));
		}
		

		Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> {
			return  (shard().size() == commitlog().maxSize()+1 && commitlog().size() == 0)
					||
					(shard().size() == commitlog().maxSize() && commitlog().size() == 1);
		});
	}

	@Test()
	public void test_existing() throws IOException {

		Configuration config = TestHelper.getConfiguration("target/translog-test-" + System.currentTimeMillis());
		

		try (CommitLog tlog = commitlog(config)) {
			tlog.open();

			tlog.append(createDoc("horst"));
			tlog.append(createDoc("klaus"));
		}

		try (CommitLog tlog = commitlog(config)) {
			tlog.open();

			Assertions.assertThat(tlog.size()).isEqualTo(2);
		}
	}

	protected IndexDocument createDoc(final String name) {
		JSONObject json = new JSONObject();
		json.put("name", name);
		json.put(Fields._UUID.value(), UUID.randomUUID().toString());
		IndexDocument doc = new IndexDocument(json);

		return doc;
	}

}
