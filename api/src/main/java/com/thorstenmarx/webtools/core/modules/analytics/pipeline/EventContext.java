package com.thorstenmarx.webtools.core.modules.analytics.pipeline;

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
import com.alibaba.fastjson.JSONObject;
import com.thorstenmarx.webtools.core.modules.analytics.db.AbstractAnalyticsDb;
import com.thorstenmarx.webtools.core.modules.analytics.util.pipeline.PipelineContext;


/**
 *
 * @author marx
 */
public class EventContext implements PipelineContext {
	
//	public static final String KEY_SCORING = "scoring";
//	public static final String KEY_MAPPING = "mapping";
    
    public static final String KEY_TIMESTAMP = "_timestamp";
	
	public final JSONObject jsonEvent;
	
	transient private AbstractAnalyticsDb db = null;
	
	public EventContext (final JSONObject event, AbstractAnalyticsDb db) {
		this.db = db;
		this.jsonEvent = event;
	}
	
	
	
	public AbstractAnalyticsDb db () {
		return db;
	}
}
