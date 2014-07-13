package com.thinkaurelius.titan.diskstorage.solr;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.core.Order;
import com.thinkaurelius.titan.core.TitanElement;
import com.thinkaurelius.titan.core.attribute.*;
import com.thinkaurelius.titan.diskstorage.*;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.indexing.*;
import com.thinkaurelius.titan.diskstorage.solr.transform.GeoToWktConverter;
import com.thinkaurelius.titan.diskstorage.util.DefaultTransaction;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.serialize.AttributeUtil;
import com.thinkaurelius.titan.graphdb.query.TitanPredicate;
import com.thinkaurelius.titan.graphdb.query.condition.And;
import com.thinkaurelius.titan.graphdb.query.condition.Condition;
import com.thinkaurelius.titan.graphdb.query.condition.Not;
import com.thinkaurelius.titan.graphdb.query.condition.PredicateCondition;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;

/**
 * @author Jared Holmberg (jholmberg@bericotechnoLogies.com)
 */
public class SolrIndex implements IndexProvider {

    public static final ConfigOption<Integer> MAX_RESULT_SET_SIZE = new ConfigOption<Integer>(INDEX_NS,"max-result-set-size",
            "Maxium number of results to return if no limit is specified",
            ConfigOption.Type.MASKABLE, 100000);

    public static final ConfigOption<Integer> COMMIT_BATCH_SIZE = new ConfigOption<Integer>(INDEX_NS,"commit-batch-size",
            "Commit batch size",
            ConfigOption.Type.MASKABLE, 1000);

    public static final ConfigOption<String[]> KEY_FIELD_NAMES = new ConfigOption<String[]>(INDEX_NS,"key-field-names",
            "Field name that uniquely identifies each document in Solr",
            ConfigOption.Type.GLOBAL_OFFLINE, new String[]{"docid"});

    public static final ConfigOption<Integer> NUM_SHARDS = new ConfigOption<Integer>(INDEX_NS,"num-shards",
            "Number of shards",
            ConfigOption.Type.GLOBAL_OFFLINE, 5);

    public static final ConfigOption<Integer> MAX_SHARDS_PER_NODE = new ConfigOption<Integer>(INDEX_NS,"max-shards-per-node",
            "Maximum number of shards per node",
            ConfigOption.Type.GLOBAL_OFFLINE, 2);

    public static final ConfigOption<Integer> REPLICATION_FACTOR = new ConfigOption<Integer>(INDEX_NS,"replication-factor",
            "Number of shards",
            ConfigOption.Type.GLOBAL_OFFLINE, 2);

    public static final ConfigOption<String> ZOOKEEPER_URL = new ConfigOption<String>(INDEX_NS,"zookeeper-url",
            "Http connection max connections per host",
            ConfigOption.Type.GLOBAL_OFFLINE, "http://localhost:2181");

    private static int batchSize;
    private static int maxResultSetSize;

    private static final Logger Log = LoggerFactory.getLogger(SolrIndex.class);

    /**
     * Builds a mapping between the core name and its respective Solr Server connection.
     */
    CloudSolrServer solrServer;
    private Map<String, String> keyFieldIds;

    /**
     *  There are several different modes in which the index can be configured with Solr:
     *  <ol>
     *    <li>EmbeddedSolrServer - used when Solr runs in same JVM as titan. Good for development but not encouraged</li>
     *    <li>HttpSolrServer - used to connect to Solr instance via Apache HTTP client to a specific solr instance bound to a specific URL.</li>
     *    <li>CloudSolrServer - used to connect to a SolrCloud cluster that uses Apache Zookeeper. This lets clients hit one host and Zookeeper distributes queries and writes automatically</li>
     *  </ol>
     *  <p>
     *      An example follows in configuring Solr support for Titan::
     *      <pre>
     *          {@code
     *              import org.apache.commons.configuration.Configuration;
     *              import static com.thinkaurelius.titan.diskstorage.solr.SolrSearchConstants.*;
     *              import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
     *
     *              public class MyClass {
     *                  private Configuration config;
     *
     *                  public MyClass(String mode) {
     *                      config = new BaseConfiguration()
     *                      if (mode.equals(SOLR_MODE_EMBEDDED)) {
     *                          config.setProperty(GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY, StorageSetup.getHomeDir("solr"));
     *                          String home = "titan-solr/target/test-classes/solr";
     *                          config.setProperty(SOLR_MODE, SOLR_MODE_EMBEDDED);
     *                          config.setProperty(SOLR_CORE_NAMES, "core1,core2,core3");
     *                          config.setProperty(SOLR_HOME, home);
     *
     *                      } else if (mode.equals(SOLR_MODE_HTTP)) {
     *                          config.setProperty(SOLR_MODE, SOLR_MODE_HTTP);
     *                          config.setProperty(SOLR_HTTP_URL, "http://localhost:8983/solr");
     *                          config.setProperty(SOLR_HTTP_CONNECTION_TIMEOUT, 10000); //in milliseconds
     *
     *                      } else if (mode.equals(SOLR_MODE_CLOUD)) {
     *                          config.setProperty(SOLR_MODE, SOLR_MODE_CLOUD);
     *                          //Don't add the protocol: http:// or https:// to the url
     *                          config.setProperty(SOLR_CLOUD_ZOOKEEPER_URL, "localhost:2181")
     *                          //Set the default collection for Solr in Zookeeper.
     *                          //Titan allows for more but just needs a default one as a fallback
     *                          config.setProperty(SOLR_CLOUD_COLLECTION, "store");
     *                      }
     *
     *                      config.setProperty(SOLR_CORE_NAMES, "store,store1,store2,store3");
     *                      //A key/value list where key is the core name and value us the name of the field used in solr to uniquely identify a document.
     *                      config.setProperty(SOLR_KEY_FIELD_NAMES, "store=document_id,store1=document_id,store2=document_id,store3=document_id");
     *                  }
     *              }
     *          }
     *      </pre>
     *  </p>
     *  <p>
     *      Something to keep in mind when using Solr as the {@link com.thinkaurelius.titan.diskstorage.indexing.IndexProvider} for Titan. Solr has many different
     *      types of indexes for backing your field types defined in the schema. Whenever you use a solr.Textfield type, string values are split up into individual
     *      tokens. This is usually desirable except in cases where you are searching for a phrase that begins with a specified prefix as in
     *      the {@link com.thinkaurelius.titan.core.attribute.Text#PREFIX} enumeration that can be used in gremlin searches. In that case, the SolrIndex will use the
     *      convention of assuming you have defined a field of the same name as the solr.Textfield but will be of type solr.Strfield.
     *  </p>
     *  <p>
     *      For example, let's say you have two documents in Solr with a field called description. One document has a description of "Tomorrow is the world", the other, "World domination".
     *      If you defined the description field in your schema and set it to type solr.TextField a PREFIX based search like the one below would return both documents:
     *      <pre>
     *          {@code
     *          g.query().has("description",Text.PREFIX,"World")
     *          }
     *      </pre>
     *  </p>
     *  <p>
     *      However, if you create a copyField with the name "descriptionString" and set its type to solr.StrField, the PREFIX search defined above would behave as expected
     *      and only return the document with description "World domination" as its a raw string that is not tokenized in the index.
     *  </p>
     * @param config Titan configuration passed in at start up time
     */
    public SolrIndex(Configuration config) throws StorageException {
        try {
            String zookeeperUrl = config.get(SolrIndex.ZOOKEEPER_URL);
            String collectionName = config.get(GraphDatabaseConfiguration.INDEX_NAME);

            solrServer = new CloudSolrServer(zookeeperUrl, true);
            solrServer.connect();
            createCollectionIfNotExists(solrServer, config);
            solrServer.setDefaultCollection(collectionName);

            waitForRecoveriesToFinish(solrServer, collectionName);
        } catch (IOException e) {
            throw new PermanentStorageException(e);
        } catch (SolrServerException e) {
            throw new PermanentStorageException(e);
        } catch (InterruptedException e) {
            throw new PermanentStorageException(e);
        } catch (KeeperException e) {
            throw new PermanentStorageException(e);
        }

        keyFieldIds = parseKeyFieldsForCores(config);

        batchSize = config.get(COMMIT_BATCH_SIZE);
        maxResultSetSize = config.get(MAX_RESULT_SET_SIZE);
    }

    private Map<String, String> parseKeyFieldsForCores(Configuration config) throws StorageException {
        Map<String, String> keyFieldNames = new HashMap<String, String>();
        String[] coreFieldStatements = config.get(KEY_FIELD_NAMES);
        for (String coreFieldStatement : coreFieldStatements) {
            String[] parts = coreFieldStatement.trim().split("=");
            if (parts.length != 2) {
                throw new PermanentStorageException("Unable to parse the core name / key field name pair. It should be of the format core=field");
            }
            String coreName = parts[0];
            String keyFieldName = parts[1];
            keyFieldNames.put(coreName, keyFieldName);
        }
        return keyFieldNames;
    }

    /**
     * Unlike the ElasticSearch Index, which is schema free, Solr requires a schema to
     * support searching. This means that you will need to modify the solr schema with the
     * appropriate field definitions in order to work properly.  If you have a running instance
     * of Solr and you modify its schema with new fields, don't forget to re-index!
     * @param store Index store
     * @param key New key to register
     * @param information Datatype to register for the key
     * @param tx enclosing transaction
     * @throws StorageException
     */
    @Override
    public void register(String store, String key, KeyInformation information, BaseTransaction tx) throws StorageException {
        //Since all data types must be defined in the schema.xml, pre-registering a type does not work
    }

    @Override
    public void mutate(Map<String, Map<String, IndexMutation>> mutations, KeyInformation.IndexRetriever informations, BaseTransaction tx) throws StorageException {
        //TODO: research usage of the informations parameter
        try {
            List<String> deleteIds = new ArrayList<String>();
            Collection<SolrInputDocument> newDocuments = new ArrayList<SolrInputDocument>();
            Collection<SolrInputDocument> updateDocuments = new ArrayList<SolrInputDocument>();
            boolean isLastBatch = false;

            for (Map.Entry<String, Map<String, IndexMutation>> stores : mutations.entrySet()) {
                String coreName = stores.getKey();
                String keyIdField = keyFieldIds.get(coreName);
                int numProcessed = 0;

                for (Map.Entry<String, IndexMutation> entry : stores.getValue().entrySet()) {
                    String docId = entry.getKey();
                    IndexMutation mutation = entry.getValue();
                    Preconditions.checkArgument(!(mutation.isNew() && mutation.isDeleted()));
                    Preconditions.checkArgument(!mutation.isNew() || !mutation.hasDeletions());
                    Preconditions.checkArgument(!mutation.isDeleted() || !mutation.hasAdditions());

                    //Handle any deletions
                    if (mutation.hasDeletions()) {
                        if (mutation.isDeleted()) {
                            Log.trace("Deleting entire document {}", docId);
                            deleteIds.add(docId);
                        } else {
                            HashSet<IndexEntry> fieldDeletions = Sets.newHashSet(mutation.getDeletions());
                            deleteIndividualFieldsFromIndex(keyIdField, docId, fieldDeletions);
                        }
                    } else {
                        HashSet<IndexEntry> fieldDeletions = Sets.newHashSet(mutation.getDeletions());
                        if (mutation.hasAdditions()) {
                            for (IndexEntry indexEntry : mutation.getAdditions()) {
                                fieldDeletions.remove(indexEntry);
                            }
                        }
                        deleteIndividualFieldsFromIndex(keyIdField, docId, fieldDeletions);
                    }

                    if (mutation.hasAdditions()) {
                        List<IndexEntry> additions = mutation.getAdditions();
                        if (mutation.isNew()) { //Index
                            Log.trace("Adding new document {}", docId);
                            SolrInputDocument newDoc = new SolrInputDocument();
                            newDoc.addField(keyIdField, docId);
                            for (IndexEntry ie : additions) {
                                Object fieldValue = ie.value;
                                if (fieldValue instanceof Geoshape) {
                                    fieldValue = GeoToWktConverter.convertToWktString((Geoshape) fieldValue);
                                }
                                newDoc.addField(ie.field, fieldValue);
                            }
                            newDocuments.add(newDoc);

                        } else { //Update
                            boolean needUpsert = !mutation.hasDeletions();
                            SolrInputDocument updateDoc = new SolrInputDocument();
                            updateDoc.addField(keyIdField, docId);
                            for (IndexEntry ie : additions) {
                                Map<String, String> updateFields = new HashMap<String, String>();
                                Object fieldValue = ie.value;
                                if (fieldValue instanceof Geoshape) {
                                    fieldValue = GeoToWktConverter.convertToWktString((Geoshape) fieldValue);
                                }
                                updateFields.put("set", fieldValue.toString());
                                updateDoc.addField(ie.field, updateFields);
                            }

                            updateDocuments.add(updateDoc);
                        }
                    }
                    numProcessed++;
                    if (numProcessed == stores.getValue().size()) {
                        isLastBatch = true;
                    }

                    commitDeletes(deleteIds, isLastBatch);
                    commitDocumentChanges(newDocuments, isLastBatch);
                    commitDocumentChanges(updateDocuments, isLastBatch);
                }
            }
        } catch (Exception e) {
            throw storageException(e);
        }
    }

    private void deleteIndividualFieldsFromIndex(String keyIdField, String docId, HashSet<IndexEntry> fieldDeletions) throws SolrServerException, IOException {
        if (!fieldDeletions.isEmpty()) {
            Map<String, String> fieldDeletes = new HashMap<String, String>();
            fieldDeletes.put("set", null);
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField(keyIdField, docId);
            StringBuilder sb = new StringBuilder();
            for (IndexEntry fieldToDelete : fieldDeletions) {
                doc.addField(fieldToDelete.field, fieldDeletes);
                sb.append(fieldToDelete).append(",");
            }
            Log.trace("Deleting individual fields [{}] for document {}", sb.toString(), docId);
            solrServer.add(doc);
            solrServer.commit();
        }
    }

    private void commitDocumentChanges(Collection<SolrInputDocument> documents, boolean isLastBatch) throws SolrServerException, IOException {
        int numUpdates = documents.size();
        if (numUpdates == 0) {
            return;
        }

        try {
            if (numUpdates >= batchSize || isLastBatch) {
                solrServer.add(documents);
                solrServer.commit();
                documents.clear();
            }
        } catch (HttpSolrServer.RemoteSolrException rse) {
            Log.error("Unable to save documents to Solr as one of the shape objects stored were not compatible with Solr.", rse);
            Log.error("Details in failed document batch: ");
            for (SolrInputDocument d : documents) {
                Collection<String> fieldNames = d.getFieldNames();
                for (String name : fieldNames) {
                    Log.error(name + ":" + d.getFieldValue(name).toString());
                }
            }

            throw rse;
        }
    }

    private void commitDeletes(List<String> deleteIds, boolean isLastBatch) throws SolrServerException, IOException {
        int numDeletes = deleteIds.size();
        if (numDeletes == 0) {
            return;
        }

        if (numDeletes >= batchSize || isLastBatch) {
            solrServer.deleteById(deleteIds);
            solrServer.commit();
            deleteIds.clear();
        }
    }

    @Override
    public List<String> query(IndexQuery query, KeyInformation.IndexRetriever informations, BaseTransaction tx) throws StorageException {
        List<String> result;
        String core = query.getStore();
        String keyIdField = keyFieldIds.get(core);
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setQuery("*:*");
        solrQuery = buildQuery(solrQuery, query.getCondition());
        if (!query.getOrder().isEmpty()) {
            List<IndexQuery.OrderEntry> orders = query.getOrder();
            for (int i = 0; i < orders.size(); i++) {
                String item = orders.get(i).getKey();
                SolrQuery.ORDER order = orders.get(i).getOrder() == Order.ASC ? SolrQuery.ORDER.asc : SolrQuery.ORDER.desc;
                solrQuery.addSort(new SolrQuery.SortClause(item, order));
            }
        }
        solrQuery.setStart(0);
        if (query.hasLimit()) {
            solrQuery.setRows(query.getLimit());
        } else {
            solrQuery.setRows(maxResultSetSize);
        }
        try {
            QueryResponse response = solrServer.query(solrQuery);
            Log.debug("Executed query [{}] in {} ms", query.getCondition(), response.getElapsedTime());
            int totalHits = response.getResults().size();
            if (!query.hasLimit() && totalHits >= maxResultSetSize) {
                Log.warn("Query result set truncated to first [{}] elements for query: {}", MAX_RESULT_SET_SIZE, query);
            }
            result = new ArrayList<String>(totalHits);
            for (SolrDocument hit : response.getResults()) {
                result.add(hit.getFieldValue(keyIdField).toString());
            }

        } catch (HttpSolrServer.RemoteSolrException e) {
            Log.error("Query did not complete because parameters were not recognized : ", e);
            throw new PermanentStorageException(e);
        } catch (SolrServerException e) {
            Log.error("Unable to query Solr index.", e);
            throw new PermanentStorageException(e);
        }
        return result;
    }

    @Override
    public Iterable<RawQuery.Result<String>> query(RawQuery query, KeyInformation.IndexRetriever informations, BaseTransaction tx) throws StorageException {
        List<RawQuery.Result<String>> result;
        String core = query.getStore();
        String keyIdField = keyFieldIds.get(core);
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setQuery("*:*");
        solrQuery.addFilterQuery(query.getQuery());
        solrQuery.addField(keyIdField);
        solrQuery.addField("score");
        solrQuery.setStart(query.getOffset());
        if (query.hasLimit()) {
            solrQuery.setRows(query.getLimit());
        } else {
            solrQuery.setRows(maxResultSetSize);
        }

        try {
            QueryResponse response = solrServer.query(solrQuery);
            Log.debug("Executed query [{}] in {} ms", query.getQuery(), response.getElapsedTime());
            int totalHits = response.getResults().size();
            if (!query.hasLimit() && totalHits >= maxResultSetSize) {
                Log.warn("Query result set truncated to first [{}] elements for query: {}", MAX_RESULT_SET_SIZE, query);
            }
            result = new ArrayList<RawQuery.Result<String>>(totalHits);

            for (SolrDocument hit : response.getResults()) {
                double score = Double.parseDouble(hit.getFieldValue("score").toString());
                result.add(
                        new RawQuery.Result<String>(hit.getFieldValue(keyIdField).toString(), score));
            }
        } catch (HttpSolrServer.RemoteSolrException e) {
            Log.error("Query did not complete because parameters were not recognized : ", e);
            throw new PermanentStorageException(e);
        } catch (SolrServerException e) {
            Log.error("Unable to query Solr index.", e);
            throw new PermanentStorageException(e);
        }
        return result;
    }


    public SolrQuery buildQuery(SolrQuery q, Condition<TitanElement> condition) {
        if (condition instanceof PredicateCondition) {
            PredicateCondition<String, TitanElement> atom = (PredicateCondition<String, TitanElement>) condition;
            Object value = atom.getValue();
            String key = atom.getKey();
            TitanPredicate titanPredicate = atom.getPredicate();

            if (value instanceof Number
                //|| value instanceof Interval
                ) {

                Preconditions.checkArgument(titanPredicate instanceof Cmp, "Relation not supported on numeric types: " + titanPredicate);
                Cmp numRel = (Cmp) titanPredicate;
                //if (numRel == Cmp.INTERVAL) {
                //    Interval i = (Interval)value;
                //    q.addFilterQuery(key + ":[" + i.getStart() + " TO " + i.getEnd() + "]");
                //    return q;
                //} else {
                    switch (numRel) {
                        case EQUAL:
                            q.addFilterQuery(key + ":" + value.toString());
                            return q;
                        case NOT_EQUAL:
                            q.addFilterQuery("-" + key + ":" + value.toString());
                            return q;
                        case LESS_THAN:
                            //use right curly to mean up to but not including value
                            q.addFilterQuery(key + ":[* TO " + value.toString() + "}");
                            return q;
                        case LESS_THAN_EQUAL:
                            q.addFilterQuery(key + ":[* TO " + value.toString() + "]");
                            return q;
                        case GREATER_THAN:
                            //use left curly to mean greater than but not including value
                            q.addFilterQuery(key + ":{" + value.toString() + " TO *]");
                            return q;
                        case GREATER_THAN_EQUAL:
                            q.addFilterQuery(key + ":[" + value.toString() + " TO *]");
                            return q;
                        default: throw new IllegalArgumentException("Unexpected relation: " + numRel);
                    }
                //}
            } else if (value instanceof String) {
                if (titanPredicate == Text.CONTAINS) {
                    //e.g. - if terms tomorrow and world were supplied, and fq=text:(tomorrow  world)
                    //sample data set would return 2 documents: one where text = Tomorrow is the World,
                    //and the second where text = Hello World
                    q.addFilterQuery(key + ":("+((String) value).toLowerCase()+")");
                    return q;
                } else if (titanPredicate == Text.PREFIX) {
                    String prefixConventionName = "String";
                    q.addFilterQuery(key + prefixConventionName + ":" + value + "*");
                    return q;
                } else if (titanPredicate == Text.REGEX) {
                    String prefixConventionName = "String";
                    q.addFilterQuery(key + prefixConventionName + ":/" + value + "/");
                    return q;
                } else if (titanPredicate == Text.CONTAINS_PREFIX) {
                    q.addFilterQuery(key + ":" + value + "*");
                    return q;
                } else if (titanPredicate == Cmp.EQUAL) {
                    q.addFilterQuery(key + ":\"" + value + "\"");
                    return q;
                } else if (titanPredicate == Cmp.NOT_EQUAL) {
                    q.addFilterQuery("-" + key + ":\"" + value + "\"");
                    return q;
                } else if (titanPredicate == Text.CONTAINS_REGEX) {
                    q.addFilterQuery(key + ":/" + value + "/");
                    return q;
                } else {
                    throw new IllegalArgumentException("Relation is not supported for string value: " + titanPredicate);
                }
            } else if (value instanceof Geoshape) {
                Geoshape geo = (Geoshape)value;
                if (geo.getType() == Geoshape.Type.CIRCLE) {
                    Geoshape.Point center = geo.getPoint();
                    q.addFilterQuery("{!geofilt sfield=" + key +
                            " pt=" + center.getLatitude() + "," + center.getLongitude() +
                            " d=" + geo.getRadius() + "} distErrPct=0"); //distance in kilometers
                    return q;
                } else if (geo.getType() == Geoshape.Type.BOX) {
                    Geoshape.Point southwest = geo.getPoint(0);
                    Geoshape.Point northeast = geo.getPoint(1);
                    q.addFilterQuery(key + ":[" + southwest.getLatitude() + "," + southwest.getLongitude() +
                                    " TO " + northeast.getLatitude() + "," + northeast.getLongitude() + "]");
                    return q;
                } else if (geo.getType() == Geoshape.Type.POLYGON) {
                    List<Geoshape.Point> coordinates = getPolygonPoints(geo);
                    StringBuilder poly = new StringBuilder(key + ":\"IsWithin(POLYGON((");
                    for (Geoshape.Point coordinate : coordinates) {
                        poly.append(coordinate.getLongitude()).append(" ").append(coordinate.getLatitude()).append(", ");
                    }
                    //close the polygon with the first coordinate
                    poly.append(coordinates.get(0).getLongitude()).append(" ").append(coordinates.get(0).getLatitude());
                    poly.append(")))\" distErrPct=0");
                    q.addFilterQuery(poly.toString());
                    return q;
                }
            }
        } else if (condition instanceof Not) {
            String[] filterConditions = q.getFilterQueries();
           for (String filterCondition : filterConditions) {
                //if (filterCondition.contains(key)) {
                    q.removeFilterQuery(filterCondition);
                    q.addFilterQuery("-" + filterCondition);
                //}
            }
            return q;
        } else if (condition instanceof And) {

            for (Condition<TitanElement> c : condition.getChildren()) {
                SolrQuery andCondition = new SolrQuery();
                andCondition.setQuery("*:*");
                andCondition =  buildQuery(andCondition, c);
                String[] andFilterConditions = andCondition.getFilterQueries();
                for (String filter : andFilterConditions) {
                    //+ in solr makes the condition required
                    q.addFilterQuery("+" + filter);
                }
            }
            return q;
        } else {
            throw new IllegalArgumentException("Invalid condition: " + condition);
        }
        return null;
    }

    private List<Geoshape.Point> getPolygonPoints(Geoshape polygon) {
        List<Geoshape.Point> locations = new ArrayList<Geoshape.Point>();

        int index = 0;
        boolean hasCoordinates = true;
        while (hasCoordinates) {
            try {
                locations.add(polygon.getPoint(index));
            } catch (ArrayIndexOutOfBoundsException ignore) {
                //just means we asked for a point past the size of the list
                //of known coordinates
                hasCoordinates = false;
            }
        }

        return locations;
    }

    /**
     * Solr handles all transactions on the server-side. That means all
     * commit, optimize, or rollback applies since the last commit/optimize/rollback.
     * Solr documentation recommends best way to update Solr is in one process to avoid
     * race conditions.
     *
     * @return New Transaction Handle
     * @throws StorageException
     */
    @Override
    public BaseTransactionConfigurable beginTransaction(BaseTransactionConfig config) throws StorageException {
        return new DefaultTransaction(config);
    }

    @Override
    public void close() throws StorageException {
        Log.trace("Shutting down connection to Solr", solrServer);
        solrServer.shutdown();
        solrServer = null;
    }

    @Override
    public void clearStorage() throws StorageException {
        try {
            Log.trace("Clearing storage from Solr", solrServer);
            solrServer.deleteByQuery("*:*");
            solrServer.commit();
        } catch (SolrServerException e) {
            Log.error("Unable to clear storage from index due to server error on Solr.", e);
            throw new PermanentStorageException(e);
        } catch (IOException e) {
            Log.error("Unable to clear storage from index due to low-level I/O error.", e);
            throw new PermanentStorageException(e);
        } catch (Exception e) {
            Log.error("Unable to clear storage from index due to general error.", e);
            throw new PermanentStorageException(e);
        }
    }

    @Override
    public boolean supports(KeyInformation information, TitanPredicate titanPredicate) {
        Class<?> dataType = information.getDataType();

        if (Number.class.isAssignableFrom(dataType)) {
            return titanPredicate instanceof Cmp;
        } else if (dataType == Geoshape.class) {
            return titanPredicate == Geo.WITHIN;
        } else {
            return  dataType == String.class && (
                    titanPredicate == Text.CONTAINS ||
                    titanPredicate == Text.PREFIX ||
                    titanPredicate == Text.REGEX ||
                    titanPredicate == Text.CONTAINS_PREFIX ||
                    titanPredicate == Text.CONTAINS_REGEX ||
                    titanPredicate == Cmp.EQUAL ||
                    titanPredicate == Cmp.NOT_EQUAL);
        }
    }

    @Override
    public boolean supports(KeyInformation information) {
        Class<?> dataType = information.getDataType();
        if (Number.class.isAssignableFrom(dataType) || dataType == Geoshape.class) {
            return true;
        } else if (AttributeUtil.isString(dataType)) {
            return true;
        }
        return false;
    }

    private StorageException storageException(Exception solrException) {
        return new TemporaryStorageException("Unable to complete query on Solr.", solrException);
    }

    private static void createCollectionIfNotExists(CloudSolrServer server, Configuration config)
            throws IOException, SolrServerException, KeeperException, InterruptedException {
        String collection = config.get(GraphDatabaseConfiguration.INDEX_NAME);

        if (!checkIfCollectionExists(server, collection)) {
            Integer numShards = config.get(NUM_SHARDS);
            Integer maxShardsPerNode = config.get(MAX_SHARDS_PER_NODE);
            Integer replicationFactor = config.get(REPLICATION_FACTOR);

            CollectionAdminRequest.Create createRequest = new CollectionAdminRequest.Create();

            createRequest.setCollectionName(collection);
            createRequest.setNumShards(numShards);
            createRequest.setMaxShardsPerNode(maxShardsPerNode);
            createRequest.setReplicationFactor(replicationFactor);

            CollectionAdminResponse createResponse = createRequest.process(server);
            if (createResponse.isSuccess()) {
                Log.trace("Collection {} successfully created.", collection);
            } else {
                throw new SolrServerException(Joiner.on("\n").join(createResponse.getErrorMessages()));
            }
        }

        waitForRecoveriesToFinish(server, collection);
    }

    /**
     * Checks if the collection has already been created in Solr.
     */
    private static boolean checkIfCollectionExists(CloudSolrServer server, String collection) throws KeeperException, InterruptedException {
        ZkStateReader zkStateReader = server.getZkStateReader();
        zkStateReader.updateClusterState(true);
        ClusterState clusterState = zkStateReader.getClusterState();
        return clusterState.getCollectionOrNull(collection) != null;
    }

    /**
     * Wait for all the collection shards to be ready.
     */
    private static void waitForRecoveriesToFinish(CloudSolrServer server, String collection) throws KeeperException, InterruptedException {
        ZkStateReader zkStateReader = server.getZkStateReader();
        try {
            boolean cont = true;

            while (cont) {
                boolean sawLiveRecovering = false;
                zkStateReader.updateClusterState(true);
                ClusterState clusterState = zkStateReader.getClusterState();
                Map<String, Slice> slices = clusterState.getSlicesMap(collection);
                Preconditions.checkNotNull("Could not find collection:" + collection, slices);

                for (Map.Entry<String, Slice> entry : slices.entrySet()) {
                    Map<String, Replica> shards = entry.getValue().getReplicasMap();
                    for (Map.Entry<String, Replica> shard : shards.entrySet()) {
                        String state = shard.getValue().getStr(ZkStateReader.STATE_PROP);
                        if ((state.equals(ZkStateReader.RECOVERING)
                                || state.equals(ZkStateReader.SYNC) || state
                                .equals(ZkStateReader.DOWN))
                                && clusterState.liveNodesContain(shard.getValue().getStr(
                                ZkStateReader.NODE_NAME_PROP))) {
                            sawLiveRecovering = true;
                        }
                    }
                }
                if (!sawLiveRecovering) {
                    cont = false;
                } else {
                    Thread.sleep(1000);
                }
            }
        } finally {
            Log.info("Exiting solr wait");
        }
    }
}
