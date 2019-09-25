/**
 * WebTools-Platform
 * Copyright (C) 2016-2018  ThorstenMarx (kontakt@thorstenmarx.com)
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
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.thorstenmarx.webtools.analytics.db;

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

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import org.testng.annotations.Test;

/**
 *
 * @author marx
 */
public class TimeTest {
	
	@Test
	public void timeTest () {
		System.out.println(Instant.now().toEpochMilli());
		System.out.println(System.currentTimeMillis());
	}
	
	@Test
	public void testFormat () {
		ZonedDateTime utc = ZonedDateTime.now(ZoneOffset.UTC);
		String formatted = utc.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
		ZonedDateTime ldt = ZonedDateTime.parse(formatted, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
		Instant instant = Instant.from(ldt);
		
		System.out.println("system: " + System.currentTimeMillis());
		System.out.println("instant: " + instant.toEpochMilli());
	}
}
