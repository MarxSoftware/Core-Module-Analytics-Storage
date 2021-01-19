/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.marx_software.webtools.core.modules.analytics.db.index.lucene.shard.migration;

import com.alibaba.fastjson.JSONObject;
import de.marx_software.webtools.core.modules.analytics.db.index.lucene.ShardVersion;
import de.marx_software.webtools.core.modules.analytics.db.index.lucene.shard.LuceneShard;
import de.marx_software.webtools.core.modules.analytics.db.index.lucene.shard.Migrator;
import java.io.IOException;
import java.util.function.Consumer;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author marx
 */
public class ShardVersion1Migration implements Migrator {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ShardVersion1Migration.class);
	
	@Override
	public void migrate (final LuceneShard shard, final Consumer<JSONObject> indexer) {
		/*
		1. Make backup
		2. delete old index
		3. iterate through content store index values
		*/
		shard.getContentStore().forEach((key, value) -> {
			JSONObject document = JSONObject.parseObject(value);
			indexer.accept(document);
		});
	}

	@Override
	public ShardVersion shardVersion() {
		return ShardVersion.SHARD_2;
	}

	@Override
	public Version luceneVersion() {
		return Version.LUCENE_8_6_1;
	}
}
