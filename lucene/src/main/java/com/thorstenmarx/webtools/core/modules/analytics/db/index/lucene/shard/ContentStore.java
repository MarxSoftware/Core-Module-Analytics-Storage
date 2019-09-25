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
import java.io.IOException;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import static org.iq80.leveldb.impl.Iq80DBFactory.*;

/**
 *
 * @author marx
 */
public class ContentStore implements AutoCloseable {

	private DB db;
	private final File parent;

	protected ContentStore (final File parent) {
		this.parent = parent;
	}
	
	protected void open () throws IOException {
		Options options = new Options();
		options.createIfMissing(true);
		options.compressionType(CompressionType.SNAPPY);
		db = factory.open(new File(parent, "content"), options);
	}

	@Override
	public void close() throws IOException {
		db.close();
	}
	
	public void put (final String uuid, final String source) {
		db.put(bytes(uuid), bytes(source));
	}
	public String get (final String uuid) {
		return asString(db.get(bytes(uuid)));
	}
}
