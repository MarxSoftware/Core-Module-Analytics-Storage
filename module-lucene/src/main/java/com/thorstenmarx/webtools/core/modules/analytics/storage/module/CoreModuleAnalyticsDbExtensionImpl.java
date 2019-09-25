package com.thorstenmarx.webtools.core.modules.analytics.storage.module;

import com.thorstenmarx.modules.api.annotation.Extension;
import com.thorstenmarx.webtools.api.analytics.AnalyticsDB;
import com.thorstenmarx.webtools.api.datalayer.DataLayer;
import com.thorstenmarx.webtools.api.extensions.core.CoreAnalyticsDbExtension;
import com.thorstenmarx.webtools.api.extensions.core.CoreDataLayerExtension;

/**
 *
 * @author marx
 */
@Extension(CoreAnalyticsDbExtension.class)
public class CoreModuleAnalyticsDbExtensionImpl extends CoreAnalyticsDbExtension {

	@Override
	public String getName() {
		return "CoreModule AnalyticsDb";
	}


	@Override
	public void init() {
	}

	@Override
	public AnalyticsDB getAnalyticsDb() {
		return CoreModuleAnalyticsDbModuleLifeCycle.analyticsDb;
	}
	
}
