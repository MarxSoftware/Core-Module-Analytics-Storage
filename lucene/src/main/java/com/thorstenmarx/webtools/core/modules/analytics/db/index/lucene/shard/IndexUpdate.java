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
package com.thorstenmarx.webtools.core.modules.analytics.db.index.lucene.shard;

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
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.util.Properties;
import org.apache.lucene.index.IndexUpgrader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author marx
 */
public class IndexUpdate {

	private static final Logger LOGGER = LoggerFactory.getLogger(IndexUpdate.class);

	private Version luceneVersion = Version.LATEST;

	public Version update(final Directory directory, final Properties shardConfiguration) throws IOException {
		String luceneVersionProperty = shardConfiguration.getProperty("lucene.version", null);
		if (luceneVersionProperty == null) {
			// index just created, no need to update
		} else {
			try {
				Version version = Version.parse(luceneVersionProperty);
				if (!Version.LATEST.equals(version)) {
					LOGGER.debug("you are using an old index format, try to update your index");
					return updateToLatestIndexVersion(directory);
				} else {
					LOGGER.debug("your index is uptodate, not upgraded needed");
				}
			} catch (ParseException ex) {
				LOGGER.error("", ex);
				throw new IllegalStateException("could not parse index version", ex);
			}
		}
		return Version.LATEST;
	}

	private Version updateToLatestIndexVersion(final Directory directory) throws IOException {
		LOGGER.debug("upgrade to latest index version");
		luceneVersion = Version.LATEST;
		if (directory.listAll().length == 0) {
			IndexUpgrader upgrader = new IndexUpgrader(directory);
			upgrader.upgrade();
		}
		LOGGER.debug("upgrade done");

		return luceneVersion;
	}
}
