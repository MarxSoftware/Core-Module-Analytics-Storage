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

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NRTCachingDirectory;

/**
 *
 * @author marx
 */
public class ReadOnlyIndexAccess implements IndexAccess {

	private Directory directory;

	private SearcherManager nrt_manager;

	private final Path path;

	public ReadOnlyIndexAccess(final Path path) {
		this.path = path.resolve("index");
	}

	@Override
	public Directory directory () {
		return directory;
	}
	
	@Override
	public IndexWriter indexWriter () {
		throw new UnsupportedOperationException("not implemented in read only");
	}
	
	@Override
	public SearcherManager searcherManager () {
		return this.nrt_manager;
	}
	
	@Override
	public void commit() throws IOException {
		throw new UnsupportedOperationException("not implemented in read only");
	}
	
	@Override
	public void open() throws IOException {
		directory = FSDirectory.open(path);

		final SearcherFactory sf = new SearcherFactory();
		nrt_manager = new SearcherManager(directory, sf);
	}

	@Override
	public void close() throws IOException {
		if (nrt_manager != null) {
			nrt_manager.close();
			directory.close();
		}
	}

}
