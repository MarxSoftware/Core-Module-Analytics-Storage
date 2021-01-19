package de.marx_software.webtools.core.modules.analytics.db.index.lucene.selection.hash;

/**
 *
 * Hash String to long value
 */
public interface HashFunction {
    long hash(String key);
}
