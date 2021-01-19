package de.marx_software.webtools.core.modules.analytics.storage.module;

import com.thorstenmarx.modules.api.annotation.Extension;
import de.marx_software.webtools.api.analytics.AnalyticsDB;
import de.marx_software.webtools.api.extensions.core.CoreAnalyticsDbExtension;

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
