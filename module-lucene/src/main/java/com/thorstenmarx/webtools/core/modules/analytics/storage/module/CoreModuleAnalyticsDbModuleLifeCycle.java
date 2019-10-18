/**
 * webTools-contentengine
 * Copyright (C) 2016  Thorsten Marx (kontakt@thorstenmarx.com)
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
package com.thorstenmarx.webtools.core.modules.analytics.storage.module;


import com.thorstenmarx.modules.api.ModuleLifeCycleExtension;
import com.thorstenmarx.modules.api.annotation.Extension;
import com.thorstenmarx.webtools.api.CoreModuleContext;
import com.thorstenmarx.webtools.api.analytics.AnalyticsDB;
import com.thorstenmarx.webtools.core.modules.analytics.db.cluster.ClusterAnalyticsDb;
import com.thorstenmarx.webtools.core.modules.analytics.db.Configuration;
import com.thorstenmarx.webtools.core.modules.analytics.db.DefaultAnalyticsDb;
import java.io.File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 * @author marx
*/
@Extension(ModuleLifeCycleExtension.class)
public class CoreModuleAnalyticsDbModuleLifeCycle extends ModuleLifeCycleExtension {

	private static final Logger LOGGER = LoggerFactory.getLogger(CoreModuleAnalyticsDbModuleLifeCycle.class);
	
	public static AnalyticsDB analyticsDb;
	public static DefaultAnalyticsDb internal_analyticsDb;
	public static ClusterAnalyticsDb cluster_analyticsDb;

	private CoreModuleContext getCoreModuleContext () {
		return (CoreModuleContext)getContext();
	}
	
	@Override
	public void activate() {
		Configuration config = new Configuration(configuration.getDataDir().getAbsolutePath());

		final CoreModuleContext context = getCoreModuleContext();
		
		internal_analyticsDb = new DefaultAnalyticsDb(config, context.getExecutor());
        internal_analyticsDb.open();
		
		if (getCoreModuleContext().isCluster()) {
			analyticsDb = cluster_analyticsDb;
			cluster_analyticsDb = new ClusterAnalyticsDb(internal_analyticsDb, getCoreModuleContext().getCluster());
		} else {
			analyticsDb = internal_analyticsDb;
		}
		
		getContext().serviceRegistry().register(AnalyticsDB.class, analyticsDb);
		
//		if (context.isCluster()) {
//			context.getCluster().
//		}
	}

	@Override
	public void deactivate() {
		try {
			getContext().serviceRegistry().unregister(AnalyticsDB.class, analyticsDb);
			
			internal_analyticsDb.close();
			
			if (getCoreModuleContext().isCluster()) {
				cluster_analyticsDb.close();
			}
		} catch (InterruptedException ex) {
			LOGGER.error("", ex);
		} catch (Exception ex) {
			LOGGER.error("", ex);
		}
	}

	@Override
	public void init() {

	}

	
	
	
	
}
