package de.marx_software.webtools.core.modules.analytics.db.index.lucene.shard;

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
import de.marx_software.webtools.api.analytics.Fields;
import de.marx_software.webtools.api.analytics.Searchable;
import de.marx_software.webtools.api.analytics.query.Query;
import de.marx_software.webtools.api.analytics.query.ShardDocument;
import de.marx_software.webtools.core.modules.analytics.db.Configuration;
import de.marx_software.webtools.core.modules.analytics.db.DefaultAnalyticsDb;
import de.marx_software.webtools.core.modules.analytics.db.index.IndexDocument;
import de.marx_software.webtools.core.modules.analytics.db.index.lucene.Shard;
import de.marx_software.webtools.core.modules.analytics.db.index.lucene.ShardVersion;
import de.marx_software.webtools.core.modules.analytics.db.index.lucene.commitlog.LevelDBCommitLog;
import de.marx_software.webtools.core.modules.analytics.db.index.lucene.CommitLog;
import de.marx_software.webtools.core.modules.analytics.db.index.lucene.shard.migration.Backup;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Shard is a single lucene index with a giffen start and end time.
 *
 * @author marx
 */
public class LuceneShard implements Searchable, Comparable<LuceneShard>, Shard {

	private static final Logger LOGGER = LoggerFactory.getLogger(LuceneShard.class);

	private long timeFrom;
	private long timeTo;

	protected final String name;
	private final Configuration configuration;
	protected String shard_uuid;

	protected Version luceneVersion = Version.LATEST;

	protected ShardVersion shardVersion = ShardVersion.LATEST;

	private File shardDir;

	private CommitLog commitLog;

	final DefaultAnalyticsDb adb;

	private final Properties shardConfiguration = new Properties();

	private final DocumentBuilder documentBuilder;

	private boolean closed = false;

	private IndexAccess indexAccess;

	protected ContentStore contentStore;

	public LuceneShard(final String name, final Configuration configuration, final DefaultAnalyticsDb adb) {
		this.name = name;
		this.configuration = configuration;
		this.adb = adb;

		documentBuilder = new DocumentBuilder();
	}

	public ContentStore getContentStore() {
		return contentStore;
	}

	public IndexAccess getIndexAccess() {
		return indexAccess;
	}

	private void update() throws IOException {
		// check if update is needed
		if (!shardConfiguration.containsKey("shard.uuid")) {
			return;
		}

		String shardVersionProperty = shardConfiguration.getProperty("shard.version", null);
		if (shardVersionProperty == null) {
			// no shard version means the shard data is still in wrong directory
			updateToLatestShardVersion();
		} else {
			ShardMigration shardMigration = new ShardMigration();
			if (shardMigration.migration_needed(this)) {
				LOGGER.debug("index migration necessery");
				final Migrator migrator = shardMigration.migrator(shardVersion);
				Backup backup = new Backup();
				backup.backup(Paths.get(new File(shardDir, "index").toURI()), Paths.get(shardDir.getAbsolutePath() + "/index.backup_" + backup.extension()));
				try (IndexAccess updateAccess = new ReadWriteIndexAccess(Paths.get(shardDir.toURI()), true);) {
					updateAccess.open();
					migrator.migrate(this, (doc) -> {
						try {
							add_index_document(updateAccess, doc);
						} catch (IOException ex) {
							LOGGER.error("error migrating index", ex);
							throw new RuntimeException(ex);
						}
					});
					shardVersion = migrator.shardVersion();
					luceneVersion = migrator.luceneVersion();
				}
			}
		}

	}

	@Override
	public String getName() {
		return name;
	}

	@Deprecated()
	private void updateToLatestShardVersion() {
		String indexDir = configuration.directory;
		if (!indexDir.endsWith("/")) {
			indexDir += "/";
		}
		indexDir += DefaultAnalyticsDb.ANALYTICS_DIR + "/index/" + getName();
		File[] toCopy = new File(indexDir).listFiles((f) -> !f.isDirectory() && !f.getName().equals("shard.properties"));
		new File(indexDir + "/index/").mkdir();
		for (File file : toCopy) {
			file.renameTo(new File(indexDir + "/index/" + file.getName()));
		}
	}

	public void open() throws IOException {

		String indexDir = configuration.directory;
		if (!indexDir.endsWith("/")) {
			indexDir += "/";
		}
		indexDir += DefaultAnalyticsDb.ANALYTICS_DIR + "/index/";

		shardDir = new File(indexDir + name);

		File shardPropertyFile = new File(shardDir, "shard.properties");
		if (shardPropertyFile.exists()) {
			shardConfiguration.load(new FileReader(shardPropertyFile));

			try {
				shardVersion = ShardVersion.parse(shardConfiguration.getProperty("shard.version"));
				luceneVersion = Version.parse(shardConfiguration.getProperty("lucene.version"));
			} catch (ParseException pe) {
				LOGGER.error("", pe);
				throw new RuntimeException(pe);
			}
		}

		this.commitLog = new LevelDBCommitLog(configuration, this, adb.getExecutor());
		commitLog.open();

		contentStore = new ContentStore(shardDir);
		contentStore.open();

		update();

		shard_uuid = shardConfiguration.getProperty("shard.uuid", UUID.randomUUID().toString());

		indexAccess = new ReadWriteIndexAccess(Paths.get(shardDir.toURI()));

		luceneVersion = new IndexUpdate().update(indexAccess.directory(), shardConfiguration);

		indexAccess.open();

		this.timeTo = getMaxTimestamp();
		this.timeFrom = getMinTimestamp();

		saveConfiguration();
	}

	private void saveConfiguration() throws IOException {
		File shardPropertyFile = new File(shardDir, "shard.properties");
//		shardConfiguration.put("shard.maxtime", String.valueOf(getMaxTimestamp()));
//		shardConfiguration.put("shard.mintime", String.valueOf(getMinTimestamp()));
		shardConfiguration.put("shard.maxtime", String.valueOf(timeTo));
		shardConfiguration.put("shard.mintime", String.valueOf(timeFrom));
		shardConfiguration.put("shard.name", name);
		shardConfiguration.put("shard.uuid", shard_uuid);
		shardConfiguration.put("lucene.version", luceneVersion.toString());
		shardConfiguration.put("shard.version", shardVersion.toString());
		shardConfiguration.store(new FileWriter(shardPropertyFile), "shard configuration");
	}

	public void close() throws IOException {
		if (closed) {
			return;
		}
		closed = true;
		saveConfiguration();

//		if (reopenTask != null) {
//			reopenTask.cancel(true);
//		}
		if (commitLog != null) {
			commitLog.close();
		}
		if (indexAccess != null) {
			indexAccess.close();
		}
		if (contentStore != null) {
			contentStore.close();
		}

	}

	private void commit() throws IOException {
		indexAccess.commit();
	}

	protected void add_internal(final JSONObject json) throws IOException {
		add_index_document(indexAccess, json);

		final String uuid = json.getString(Fields._UUID.value());
		final String source = json.toJSONString();
		contentStore.put(uuid, source);

	}

	private void add_index_document(final IndexAccess indexer, final JSONObject json) throws IOException {
		final Document document = documentBuilder.build(json);
		indexer.indexWriter().addDocument(document);
		long timestamp = document.getField(Fields._TimeStamp.value()).numericValue().longValue();
		if (timestamp > timeTo || timeTo == -1) {
			timeTo = timestamp;
		}
		if (timestamp < timeFrom || timeFrom == -1) {
			timeFrom = timestamp;
		}
	}

	protected void add_internal(final String json) throws IOException {
		JSONObject jsonObj = JSONObject.parseObject(json);
		add_internal(jsonObj);
	}

	@Override
	public void add(final IndexDocument indexDocument) throws IOException {
//		translog.add(indexDocument.json.toJSONString());
		add_internal(indexDocument.json);
	}

	public void addToLog(IndexDocument document) throws IOException {
		commitLog.append(document);
	}

	public void delete(Term term) throws IOException {
		indexAccess.indexWriter().deleteDocuments(term);
	}

	public void update(Term term, Document doc) throws IOException {
		indexAccess.indexWriter().updateDocument(term, doc);
	}

	@Override
	public List<ShardDocument> search(final Query query) throws IOException {
		commitLog.readLock().lock();
		IndexSearcher indexSearcher = indexAccess.searcherManager().acquire();
		try {
			return internal_search(query, indexSearcher);
		} finally {
			indexAccess.searcherManager().release(indexSearcher);
			commitLog.readLock().unlock();
		}
	}

	private List<ShardDocument> internal_search(final Query query, IndexSearcher indexSearcher) throws IOException {
		final List<ShardDocument> result = new ArrayList<>();

		lucene_search(query, indexSearcher, (source) -> {
			final JSONObject sourceJson = JSONObject.parseObject(source);
			result.add(new ShardDocument(name, sourceJson));
		});

		return Collections.unmodifiableList(result);
	}

	private void lucene_search(final Query query, final IndexSearcher indexSearcher, Consumer<String> consumer) throws IOException {

		BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();

		//NumericRangeQuery<Long> rangeQuery = NumericRangeQuery.newLongRange("timestamp_sort", query.start(), query.end(), true, true);
		org.apache.lucene.search.Query rangeQuery = LongPoint.newRangeQuery(Fields.TIMESTAMP_SORT.value(), query.start(), query.end());
		queryBuilder.add(rangeQuery, BooleanClause.Occur.FILTER);

		if (query.terms() != null && !query.terms().isEmpty()) {
			query.terms().entrySet().forEach((e) -> {
				queryBuilder.add(new TermQuery(new Term(e.getKey(), e.getValue())), BooleanClause.Occur.FILTER);
			});
		}

		if (query.multivalueTerms() != null && !query.multivalueTerms().isEmpty()) {
			query.multivalueTerms().entrySet().stream().map(LuceneShard::multivalueTermsToBooleanQuery).forEach(booleanQuery -> {
				queryBuilder.add(booleanQuery, BooleanClause.Occur.FILTER);
			});
		}

		DocumentCollector collector = new DocumentCollector(indexSearcher.getIndexReader().maxDoc());

		BooleanQuery booleanQuery = queryBuilder.build();

		indexSearcher.search(booleanQuery, collector);

		BitSet hits = collector.hits();
		hits.stream().forEach((i) -> {
			try {
				Document doc = indexSearcher.doc(i);
//				final byte[] sourceBytes = doc.getBinaryValue(Fields.SOURCE.value()).bytes;
//				final String source = Snappy.uncompressString(sourceBytes);
				final String uuid = doc.get(Fields._UUID.value());
				final String source = contentStore.get(uuid);

				consumer.accept(source);
			} catch (IOException ex) {
				LOGGER.error("", ex);
			}
		});

		// for realtime search add transaction log data
		List<ShardDocument> translogDocs = commitLog.getSearchable().search(query);

		translogDocs.forEach((doc) -> {
			consumer.accept(doc.document.toJSONString());
		});
	}

	public List<String> raw_search(final Query query) throws IOException {
		commitLog.readLock().lock();
		IndexSearcher indexSearcher = indexAccess.searcherManager().acquire();
		try {
			return raw_search(query, indexSearcher);
		} finally {
			indexAccess.searcherManager().release(indexSearcher);
			commitLog.readLock().unlock();
		}
	}

	private List<String> raw_search(final Query query, IndexSearcher indexSearcher) throws IOException {

		final List<String> result = new ArrayList<>();

		lucene_search(query, indexSearcher, (source) -> {
			result.add(source);
		});

		return Collections.unmodifiableList(result);
	}

	@Override
	public boolean hasData(long from, long to) {
		final boolean commitlogData = commitLog.getSearchable().hasData(from, to);
		return commitlogData
				|| (from >= timeFrom && from <= timeTo) // from liegt innerhalb des Shards
				|| (to >= timeFrom && to <= timeTo) // to liegt innerhalb des Shards
				|| (from <= timeFrom && to >= timeTo); // der komplette Shard ist eine Teilmenge
	}

	public long getTimeFrom() {
		return timeFrom;
	}

	public long getTimeTo() {
		return timeTo;
	}

	@Override
	public int size() {
		commitLog.readLock().lock();
		try {
			IndexSearcher localSearcher = indexAccess.searcherManager().acquire();
			try {
				return localSearcher.getIndexReader().numDocs() + commitLog.size();
			} finally {
				indexAccess.searcherManager().release(localSearcher);
			}
		} catch (IOException ex) {
			LOGGER.error("", ex);
			throw new RuntimeException(ex);
		} finally {
			commitLog.readLock().unlock();
		}
	}

	public long getMaxTimestamp() throws IOException {
		IndexSearcher localSearcher = indexAccess.searcherManager().acquire();
		try {
			return getMaxTimestamp(localSearcher.getIndexReader());
		} finally {
			indexAccess.searcherManager().release(localSearcher);
		}
	}

	private long getMaxTimestamp(IndexReader reader) throws IOException {
		long max = -1;
		try {
			for (LeafReaderContext ctx : reader.leaves()) {
				final NumericDocValues longs
						= ctx.reader().getNumericDocValues(Fields._TimeStamp.value());
				int docid = DocIdSetIterator.NO_MORE_DOCS;
				while ((docid = longs.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
					if (max == -1) {
						max = longs.longValue();
					} else {
						max = Math.max(max, longs.longValue());
					}
				}
			}
		} catch (Exception e) {
			LOGGER.error("", e);
		}

		return max;
	}

	private long getMinTimestamp() throws IOException {
		IndexSearcher localSearcher = indexAccess.searcherManager().acquire();
		try {
			return getMinTimestamp(localSearcher.getIndexReader());
		} finally {
			indexAccess.searcherManager().release(localSearcher);
		}
	}

	private long getMinTimestamp(IndexReader reader) throws IOException {
		long min = -1;
		try {
			for (LeafReaderContext ctx : reader.leaves()) {
				final NumericDocValues longs
						= ctx.reader().getNumericDocValues(Fields._TimeStamp.value());
				int docid = DocIdSetIterator.NO_MORE_DOCS;
				while ((docid = longs.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
					if (min == -1) {
						min = longs.longValue();
					} else {
						min = Math.min(min, longs.longValue());
					}
				}
//				for (int i = 0; i < ctx.reader().maxDoc(); ++i) {
//					if (longs == null) {
//						min = -1;
//					} else if (min == -1) {
//						min = longs.get(i);
//					} else {
//						min = Math.min(min, longs.get(i));
//					}
//				}
			}
		} catch (Exception e) {
			LOGGER.error("", e);
		}

		return min;
	}

	@Override
	public void reopen() {
		try {
			commit();
			indexAccess.searcherManager().maybeRefreshBlocking();
		} catch (IOException ex) {
			LOGGER.error("", ex);
		}
	}

	public void reopen_internal() {
		try {
			indexAccess.searcherManager().maybeRefresh();
		} catch (IOException ex) {
			LOGGER.error("", ex);
		}
	}

	@Override
	public int compareTo(LuceneShard t) {
		String num1 = name.substring(name.lastIndexOf("_") + 1);
		String num2 = t.name.substring(t.name.lastIndexOf("_") + 1);

		return num1.compareTo(num2);
	}

	@Override
	public int hashCode() {
		return name.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final LuceneShard other = (LuceneShard) obj;
		if (!Objects.equals(this.name, other.name)) {
			return false;
		}
		return true;
	}

	@Override
	public boolean isLocked() {
		return commitLog.isLocked();
	}

	public static BooleanQuery multivalueTermsToBooleanQuery(Map.Entry<String, String[]> entry) {
		BooleanQuery.Builder subQuery = new BooleanQuery.Builder();

		String key = entry.getKey();
		String values[] = entry.getValue();
		for (String value : values) {
			subQuery.add(new TermQuery(new Term(key, value)), BooleanClause.Occur.SHOULD);
		}
		return subQuery.build();
	}
}
