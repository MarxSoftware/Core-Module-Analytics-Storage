package com.thorstenmarx.webtools.core.modules.analytics.db.index.lucene.shard;

/*-
 * #%L
 * webtools-analytics
 * %%
 * Copyright (C) 2016 - 2018 Thorsten Marx
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
import java.util.BitSet;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SimpleCollector;

/**
 *
 * @author marx
 */
public class DocumentCollector extends SimpleCollector {

	private final BitSet hits;
	
	int docBase = 0;

	DocumentCollector(final int maxSize) {
		hits = new BitSet(maxSize);
	}

	public BitSet hits () {
		return hits;
	}
	
	@Override
	protected void doSetNextReader(LeafReaderContext context) throws IOException {
		super.doSetNextReader(context);
		this.docBase = context.docBase;
	}
	
	

	@Override
	public void collect(int doc) throws IOException {
		hits.set(docBase + doc);
	}

	@Override
	public ScoreMode scoreMode() {
		return ScoreMode.COMPLETE_NO_SCORES;
	}
}
