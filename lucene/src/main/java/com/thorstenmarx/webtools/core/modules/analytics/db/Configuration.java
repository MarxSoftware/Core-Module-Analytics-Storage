package com.thorstenmarx.webtools.core.modules.analytics.db;

import com.thorstenmarx.webtools.core.modules.analytics.db.index.lucene.translog.LevelDBTransLog;

/**
 *
 * @author marx
 */
public class Configuration {

	public final String directory;
	public final int shards;
	public final int translog_maxsize;

	public Configuration(final String directory, final int shards, final int translog_maxsize) {
		this.directory = directory;
		this.shards = shards;
		this.translog_maxsize= translog_maxsize;
	}
	public Configuration(final String directory) {
		this(directory, 10, LevelDBTransLog.DEFAULT_MAX_SIZE);
	}
	
}
