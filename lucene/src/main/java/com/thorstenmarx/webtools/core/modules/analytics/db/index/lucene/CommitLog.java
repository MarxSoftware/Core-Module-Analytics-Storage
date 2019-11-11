package com.thorstenmarx.webtools.core.modules.analytics.db.index.lucene;

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
import com.thorstenmarx.webtools.api.analytics.Searchable;
import com.thorstenmarx.webtools.core.modules.analytics.db.index.IndexDocument;
import java.io.IOException;
import java.util.concurrent.locks.Lock;

/**
 *
 * @author marx
 */
public interface CommitLog extends AutoCloseable {

	void append(final IndexDocument document) throws IOException;

	@Override
	void close() throws IOException;

	void open() throws IOException;

	int size();
	
	int maxSize ();
	
	Searchable getSearchable();
	
	public Lock readLock ();
	
	public boolean isLocked();
	
	void flush ();
	
}
