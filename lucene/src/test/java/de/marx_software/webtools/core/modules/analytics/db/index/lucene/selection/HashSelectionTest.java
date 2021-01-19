package de.marx_software.webtools.core.modules.analytics.db.index.lucene.selection;

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

import de.marx_software.webtools.core.modules.analytics.db.index.IndexDocument;
import de.marx_software.webtools.core.modules.analytics.db.index.lucene.Shard;
import java.io.IOException;
import org.assertj.core.api.Assertions;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class HashSelectionTest {
	HashShardSelectionStrategy<MockShard> round;

	private String uuid () {
		return UUID.randomUUID().toString();
	}
	
	@Test
	public void testOne() {
		
		MockShard shard1 = new MockShard("shard1");
		
		round = new HashShardSelectionStrategy(Arrays.asList(shard1));
		Assertions.assertThat(round.route(uuid())).isEqualTo(shard1);
		Assertions.assertThat(round.route(uuid())).isEqualTo(shard1);
	}

	@Test
	public void testTwo() {
		
		MockShard shard1 = new MockShard("shard1");
		MockShard shard2 = new MockShard("shard2");
		
		AtomicInteger shard1_count = new AtomicInteger(0);
		AtomicInteger shard2_count = new AtomicInteger(0);
		
		round = new HashShardSelectionStrategy(Arrays.asList(shard1, shard2));
		
		for (int i = 0; i < 1000; i++) {
			Shard selected = round.route(uuid());
			if (selected.equals(shard1)) {
				shard1_count.incrementAndGet();
			} else if (selected.equals(shard2)) {
				shard2_count.incrementAndGet();
			}
		}
		int sum = shard1_count.intValue() + shard2_count.intValue();
		
		Assertions.assertThat(sum).isEqualTo(1000);
	}
	
	static class MockShard implements Shard {
		
		final String name;
		
		public MockShard (final String name) {
			this.name = name;
		}

		@Override
		public void add(IndexDocument indexDocument) throws IOException {
			throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
		}

		@Override
		public void reopen() {
			throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
		}

		@Override
		public String getName() {
			return name;
		}

		@Override
		public int size() {
			throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
		}

		@Override
		public boolean isLocked() {
			return false;
		}
		
	}
}
