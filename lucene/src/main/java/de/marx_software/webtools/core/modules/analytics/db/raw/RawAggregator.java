/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.marx_software.webtools.core.modules.analytics.db.raw;

import java.util.List;
import java.util.concurrent.Callable;

public abstract class RawAggregator<T> implements Callable<T> {
	
	protected List<String> documents;

	protected boolean error = false;
	protected String errorMessage = null;
	
	public void documents (final List<String> documents) {
		this.documents = documents;
	}
	
	public void error (final boolean error) {
		this.error = error;
	}
	
	public void errorMessage (final String errorMessage) {
		this.errorMessage = errorMessage;
	}
}