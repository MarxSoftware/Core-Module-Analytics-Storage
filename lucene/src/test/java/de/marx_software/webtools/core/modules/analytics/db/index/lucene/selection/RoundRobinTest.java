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

public class RoundRobinTest {
	RoundRobinShardSelectionStrategy<MockShard> round;

	@Test
	public void testOne() {
		
		MockShard shard1 = new MockShard();
		
		round = new RoundRobinShardSelectionStrategy(Arrays.asList(shard1));
		Assertions.assertThat(round.next()).isEqualTo(shard1);
		Assertions.assertThat(round.next()).isEqualTo(shard1);
	}

	@Test
	public void testTwo() {
		
		MockShard shard1 = new MockShard();
		MockShard shard2 = new MockShard();
		
		
		round = new RoundRobinShardSelectionStrategy(Arrays.asList(shard1, shard2));
		Assertions.assertThat(round.next()).isEqualTo(shard1);
		Assertions.assertThat(round.next()).isEqualTo(shard2);
		Assertions.assertThat(round.next()).isEqualTo(shard1);
		Assertions.assertThat(round.next()).isEqualTo(shard2);
	}

	@Test
	public void testThree() {
		
		MockShard shard1 = new MockShard();
		MockShard shard2 = new MockShard();
		MockShard shard3 = new MockShard();
		
		round = new RoundRobinShardSelectionStrategy(Arrays.asList(shard1, shard2, shard3));
		Assertions.assertThat(round.next()).isEqualTo(shard1);
		Assertions.assertThat(round.next()).isEqualTo(shard2);
		Assertions.assertThat(round.next()).isEqualTo(shard3);
		Assertions.assertThat(round.next()).isEqualTo(shard1);
		Assertions.assertThat(round.next()).isEqualTo(shard2);
		Assertions.assertThat(round.next()).isEqualTo(shard3);
	}
	
	static class MockShard implements Shard {

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
			throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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
