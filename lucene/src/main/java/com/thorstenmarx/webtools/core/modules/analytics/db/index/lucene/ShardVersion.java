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

import java.text.ParseException;

public final class ShardVersion {

	
	public static final ShardVersion SHARD_1 = new ShardVersion(1);
	
	public static final ShardVersion SHARD_2 = new ShardVersion(2);

	public static final ShardVersion LATEST = SHARD_2;

	/**
	 * Parse a version number of the form
	 * {@code "major.minor.bugfix.prerelease"}.
	 *
	 * Part {@code ".bugfix"} and part {@code ".prerelease"} are optional. Note
	 * that this is forwards compatible: the parsed version does not have to
	 * exist as a constant.
	 *
	 * @lucene.internal
	 */
	public static ShardVersion parse(String version) throws ParseException {

		int major;
		try {
			major = Integer.parseInt(version.trim());
		} catch (NumberFormatException nfe) {
			ParseException p = new ParseException("Failed to parse major version (got: " + version + ")", 0);
			p.initCause(nfe);
			throw p;
		}

		try {
			return new ShardVersion(major);
		} catch (IllegalArgumentException iae) {
			ParseException pe = new ParseException("failed to parse version string \"" + version + "\": " + iae.getMessage(), 0);
			pe.initCause(iae);
			throw pe;
		}
	}

	/**
	 * Major version, the difference between stable and trunk
	 */
	public final int major;
	public final String majorString;

	private ShardVersion(int major) {
		this.major = major;
		this.majorString = String.valueOf(major);
	}

	/**
	 * Returns true if this version is the same or after the version from the
	 * argument.
	 */
	public boolean onOrAfter(ShardVersion other) {
		return major >= other.major;
	}

	@Override
	public String toString() {
		return "" + major;
	}

	@Override
	public boolean equals(Object o) {
		return o != null && o instanceof ShardVersion && ((ShardVersion) o).major == major;
	}


	@Override
	public int hashCode() {
		return majorString.hashCode();
	}
}
