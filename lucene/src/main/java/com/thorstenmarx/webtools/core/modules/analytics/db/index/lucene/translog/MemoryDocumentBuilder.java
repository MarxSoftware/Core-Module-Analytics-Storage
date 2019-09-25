package com.thorstenmarx.webtools.core.modules.analytics.db.index.lucene.translog;

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
import com.alibaba.fastjson.JSONObject;
import com.thorstenmarx.webtools.core.modules.analytics.db.index.lucene.shard.DocumentBuilder;
import com.thorstenmarx.webtools.core.modules.analytics.util.ObjectPool;
import java.io.IOException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.memory.MemoryIndex;

/**
 *
 * @author marx
 */
public class MemoryDocumentBuilder extends DocumentBuilder {

	ObjectPool<MemoryIndex> indexPool = new ObjectPool<MemoryIndex>() {
		@Override
		public MemoryIndex createExpensiveObject() {
			return new MemoryIndex();
		}
	};
	
	public void giveBack (final MemoryIndex index) {
		index.reset();
		indexPool.giveBack(index);
	}
	
	public MemoryIndex buildMemory(final JSONObject json) throws IOException {
//		return MemoryIndex.fromDocument(super.build(json), analyzer);
		return fromDocument(super.build(json), analyzer);
	}

	private MemoryIndex fromDocument(Iterable<? extends IndexableField> document, Analyzer analyzer) {
		MemoryIndex mi = indexPool.borrow();
		for (IndexableField field : document) {
			mi.addField(field, analyzer);
		}
		return mi;
	}
}
