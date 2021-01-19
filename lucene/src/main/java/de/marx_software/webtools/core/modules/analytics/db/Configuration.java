package de.marx_software.webtools.core.modules.analytics.db;

import com.thorstenmarx.modules.api.ModuleConfiguration;
import de.marx_software.webtools.api.CoreModuleContext;
import de.marx_software.webtools.api.ModuleContext;
import de.marx_software.webtools.core.modules.analytics.db.index.lucene.LuceneIndex;
import de.marx_software.webtools.core.modules.analytics.db.index.lucene.commitlog.LevelDBCommitLog;

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
		this(directory, LuceneIndex.DEFAULT_SHARD_COUNT, LevelDBCommitLog.DEFAULT_MAX_SIZE);
	}
	public Configuration(final ModuleConfiguration configuration, final CoreModuleContext context) {
		this(configuration.getDataDir().getAbsolutePath(), 
				context.get("analyticsdb.shard.count", Integer.class, LuceneIndex.DEFAULT_SHARD_COUNT), 
				LevelDBCommitLog.DEFAULT_MAX_SIZE);
	}
	
}
