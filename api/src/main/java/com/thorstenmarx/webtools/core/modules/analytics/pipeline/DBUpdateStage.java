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
import com.thorstenmarx.webtools.api.analytics.Filter;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.thorstenmarx.webtools.api.analytics.Fields;
import com.thorstenmarx.webtools.api.analytics.TrackedEvent;
import com.thorstenmarx.webtools.api.analytics.Versions;
import com.thorstenmarx.webtools.core.modules.analytics.db.index.IndexDocument;
import com.thorstenmarx.webtools.core.modules.analytics.util.pipeline.PipelineContext;
import com.thorstenmarx.webtools.core.modules.analytics.util.pipeline.Stage;
import java.util.List;
import java.util.UUID;
import net.engio.mbassy.bus.MBassador;

/**
 * In dieser Stage werden neue Einträge in die Datenbank geschrieben und
 * bestehende ggf. aktualisiert.
 *
 * @author marx
 */
public class DBUpdateStage implements Stage {

	private static final Logger LOGGER = Logger.getLogger(DBUpdateStage.class.getName());
	final MBassador eventBus;
	
	public DBUpdateStage (final MBassador eventBus) {
		this.eventBus = eventBus;
	}
	
	@Override
	public void execute(PipelineContext context) {
		final EventContext eventContext = (EventContext) context;

		JSONObject object = eventContext.jsonEvent;

		if (eventContext.db().hasFilters()) {
			List<Filter> filters = eventContext.db().getFilters();
			filters.forEach(filter -> filter.filter(object));
		}
		
		

		if (!object.getJSONObject("data").containsKey(Fields._UUID.value())){
			object.getJSONObject("data").put(Fields._UUID.value(), UUID.randomUUID().toString());
		}

		object.getJSONObject("data").put(Fields.VERSION.value(), Versions.latest.value());

		try {
			// update index
			eventContext.db().index().add(new IndexDocument(object.getJSONObject("data")));
			
			TrackedEvent event = new TrackedEvent(object);
			//eventBus.publishAsync(event);
		} catch (IOException ex) {
			LOGGER.log(Level.SEVERE, "", ex);
		}
	}

	
}
