/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.marx_software.webtools.core.modules.analytics.db.index.lucene.shard;

import de.marx_software.webtools.core.modules.analytics.db.index.lucene.ShardVersion;
import de.marx_software.webtools.core.modules.analytics.db.index.lucene.shard.migration.ShardVersion1Migration;

/**
 *
 * @author marx
 */
public class ShardMigration {
	
	public boolean migration_needed (final LuceneShard shard) {
		return !shard.shardVersion.onOrAfter(ShardVersion.LATEST);
	}
	
	public Migrator migrator (final ShardVersion version) {
		switch (version.major) {
			case 1:
				return new ShardVersion1Migration();				
		}
		throw new RuntimeException("no migrator found");
	}
}
