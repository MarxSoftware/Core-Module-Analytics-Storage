/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.thorstenmarx.webtools.core.modules.analytics.db.index.lucene.shard;

import com.alibaba.fastjson.JSONObject;
import com.thorstenmarx.webtools.core.modules.analytics.db.index.lucene.ShardVersion;
import java.util.function.Consumer;
import org.apache.lucene.util.Version;

/**
 *
 * @author marx
 */
public interface Migrator {
	public void migrate (final LuceneShard shard, final Consumer<JSONObject> indexer);
	
	public ShardVersion shardVersion ();
	
	public Version luceneVersion ();
}
