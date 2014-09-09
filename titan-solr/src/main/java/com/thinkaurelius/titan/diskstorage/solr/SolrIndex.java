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
import com.thinkaurelius.titan.graphdb.database.serialize.AttributeUtil;
import com.thinkaurelius.titan.graphdb.query.TitanPredicate;
import com.thinkaurelius.titan.graphdb.query.condition.And;
import com.thinkaurelius.titan.graphdb.query.condition.Condition;
import com.thinkaurelius.titan.graphdb.query.condition.Not;
import com.thinkaurelius.titan.graphdb.query.condition.PredicateCondition;

import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.*;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.impl.LBHttpSolrServer;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;

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
    private static final Logger logger = LoggerFactory.getLogger(SolrIndex.class);

    private static final String CORE_PARAM = "collection";

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
            ConfigOption.Type.GLOBAL_OFFLINE, "localhost:2181");

    public static final ConfigOption<String[]> CORES = new ConfigOption<String[]>(INDEX_NS,"cores",
            "Solr cores that are going to be used.",
            ConfigOption.Type.GLOBAL_OFFLINE, new String[] { "titan" });

    public static final ConfigOption<String> TTL_FIELD = new ConfigOption<String>(INDEX_NS,"ttl_field",
            "Name of the TTL field for Solr cores.",
            ConfigOption.Type.GLOBAL_OFFLINE, "ttl");

    public static final ConfigOption<String> SOLR_MODE = new ConfigOption<String>(INDEX_NS,"mode",
            "The operation mode for Solr (HTTP or SolrCloud)",
            ConfigOption.Type.GLOBAL_OFFLINE, "cloud");

    /** HTTP Configuration */

    public static final ConfigOption<String[]> HTTP_URLS = new ConfigOption<String[]>(INDEX_NS,"http-urls",
            "List of URLs to use to connect to Solr Servers (LBSolrServer is used), don't add core name to the URL.",
            ConfigOption.Type.GLOBAL_OFFLINE, new String[] { "http://localhost:8983/solr" });

    public static final ConfigOption<Integer> HTTP_CONNECTION_TIMEOUT = new ConfigOption<Integer>(INDEX_NS,"http-connection-timeout",
            "Solr HTTP connection timeout.",
            ConfigOption.Type.GLOBAL_OFFLINE, 5000);

    public static final ConfigOption<Boolean> HTTP_ALLOW_COMPRESSION = new ConfigOption<Boolean>(INDEX_NS,"http-compression",
            "Enable/disable compression on the HTTP connections made to Solr.",
            ConfigOption.Type.GLOBAL_OFFLINE, false);

    public static final ConfigOption<Integer> HTTP_MAX_CONNECTIONS_PER_HOST = new ConfigOption<Integer>(INDEX_NS,"http-max-per-host",
            "Maximum number of HTTP connections per Solr host.",
            ConfigOption.Type.GLOBAL_OFFLINE, 20);

    public static final ConfigOption<Integer> HTTP_GLOBAL_MAX_CONNECTIONS = new ConfigOption<Integer>(INDEX_NS,"http-max",
            "Maximum number of HTTP connections in total to all Solr servers.",
            ConfigOption.Type.GLOBAL_OFFLINE, 100);

    private static final IndexFeatures SOLR_FEATURES = new IndexFeatures.Builder().supportsDocumentTTL().build();

    /**
     * Builds a mapping between the core name and its respective Solr Server connection.
     */
    SolrServer solrServer;
    private final Map<String, String> keyFieldIds;
    private final String[] cores;
    private final String ttlField;

    private int maxResults;

    /**
     *  There are several different modes in which the index can be configured with Solr:
     *  <ol>
     *    <li>HttpSolrServer - used to connect to Solr instance via Apache HTTP client to a specific solr instance bound to a specific URL.</li>
     *    <li>
     *        CloudSolrServer - used to connect to a SolrCloud cluster that uses Apache Zookeeper.
     *                      This lets clients hit one host and Zookeeper distributes queries and writes automatically
     *    </li>
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
     *                      if (mode.equals(SOLR_MODE_HTTP)) {
     *                          config.set(SOLR_MODE, "http");
     *                          config.set(HTTP_URL, new String[] { "http://localhost:8983/solr" });
     *                          config.set(HTTP_CONNECTION_TIMEOUT, 10000); //in milliseconds
     *                      } else if (mode.equals(SOLR_MODE_CLOUD)) {
     *                          config.set(SOLR_MODE, SOLR_MODE_CLOUD);
     *                          //Don't add the protocol: http:// or https:// to the url
     *                          config.set(SOLR_CLOUD_ZOOKEEPER_URL, "localhost:2181");
     *                      }
     *
     *                      config.set(CORES, new String[] { "a", "b", "c" });
     *                      //A key/value list where key is the core name and value us the name of the field used in solr to uniquely identify a document.
     *                      config.set(KEY_FIELD_NAMES, new String[] { "store=document_id" , "store1=document_id" });
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
    public SolrIndex(final Configuration config) throws BackendException {
        String mode = config.get(SOLR_MODE);

        cores = config.get(CORES);

        try {
            if (mode.equalsIgnoreCase("cloud")) {
                String zookeeperUrl = config.get(SolrIndex.ZOOKEEPER_URL);

                CloudSolrServer cloudServer = new CloudSolrServer(zookeeperUrl, true);
                cloudServer.connect();
                createCollectionIfNotExists(cloudServer, config, cores);
                if (cores.length == 1)
                    cloudServer.setDefaultCollection(cores[0]);

                for (String collection : cores)
                    waitForRecoveriesToFinish(cloudServer, collection);

                solrServer = cloudServer;
            } else if (mode.equalsIgnoreCase("http")) {
                HttpClient clientParams = HttpClientUtil.createClient(new ModifiableSolrParams() {{
                    add(HttpClientUtil.PROP_ALLOW_COMPRESSION, config.get(HTTP_ALLOW_COMPRESSION).toString());
                    add(HttpClientUtil.PROP_CONNECTION_TIMEOUT, config.get(HTTP_CONNECTION_TIMEOUT).toString());
                    add(HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST, config.get(HTTP_MAX_CONNECTIONS_PER_HOST).toString());
                    add(HttpClientUtil.PROP_MAX_CONNECTIONS, config.get(HTTP_GLOBAL_MAX_CONNECTIONS).toString());
                }});

                solrServer = new LBHttpSolrServer(clientParams, config.get(HTTP_URLS));
            } else {
                throw new IllegalArgumentException("Unknown Solr operation mode: " + mode);
            }
        } catch (IOException e) {
            throw new PermanentBackendException(e);
        } catch (SolrServerException e) {
            throw new PermanentBackendException(e);
        } catch (InterruptedException e) {
            throw new PermanentBackendException(e);
        } catch (KeeperException e) {
            throw new PermanentBackendException(e);
        }

        keyFieldIds = parseKeyFieldsForCores(config);
        maxResults = config.get(INDEX_MAX_RESULT_SET_SIZE);
        ttlField = config.get(TTL_FIELD);
    }

    private Map<String, String> parseKeyFieldsForCores(Configuration config) throws BackendException {
        Map<String, String> keyFieldNames = new HashMap<String, String>();
        String[] coreFieldStatements = config.get(KEY_FIELD_NAMES);
        for (String coreFieldStatement : coreFieldStatements) {
            String[] parts = coreFieldStatement.trim().split("=");
            if (parts.length != 2) {
                throw new PermanentBackendException("Unable to parse the core name / key field name pair. It should be of the format core=field");
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
     * @throws com.thinkaurelius.titan.diskstorage.BackendException
     */
    @Override
    public void register(String store, String key, KeyInformation information, BaseTransaction tx) throws BackendException {
        //Since all data types must be defined in the schema.xml, pre-registering a type does not work
    }

    @Override
    public void mutate(Map<String, Map<String, IndexMutation>> mutations, KeyInformation.IndexRetriever informations, BaseTransaction tx) throws BackendException {
        try {
            for (Map.Entry<String, Map<String, IndexMutation>> stores : mutations.entrySet()) {
                String coreName = stores.getKey();
                String keyIdField = keyFieldIds.get(coreName);

                List<String> deleteIds = new ArrayList<String>();
                Collection<SolrInputDocument> changes = new ArrayList<SolrInputDocument>();

                for (Map.Entry<String, IndexMutation> entry : stores.getValue().entrySet()) {
                    String docId = entry.getKey();
                    IndexMutation mutation = entry.getValue();
                    Preconditions.checkArgument(!(mutation.isNew() && mutation.isDeleted()));
                    Preconditions.checkArgument(!mutation.isNew() || !mutation.hasDeletions());
                    Preconditions.checkArgument(!mutation.isDeleted() || !mutation.hasAdditions());

                    //Handle any deletions
                    if (mutation.hasDeletions()) {
                        if (mutation.isDeleted()) {
                            logger.trace("Deleting entire document {}", docId);
                            deleteIds.add(docId);
                        } else {
                            HashSet<IndexEntry> fieldDeletions = Sets.newHashSet(mutation.getDeletions());
                            deleteIndividualFieldsFromIndex(coreName, keyIdField, docId, fieldDeletions);
                        }
                    } else {
                        HashSet<IndexEntry> fieldDeletions = Sets.newHashSet(mutation.getDeletions());
                        if (mutation.hasAdditions()) {
                            for (IndexEntry indexEntry : mutation.getAdditions()) {
                                fieldDeletions.remove(indexEntry);
                            }
                        }
                        deleteIndividualFieldsFromIndex(coreName, keyIdField, docId, fieldDeletions);
                    }

                    if (mutation.hasAdditions()) {
                        int ttl = mutation.determineTTL();

                        SolrInputDocument doc = new SolrInputDocument();
                        doc.setField(keyIdField, docId);

                        boolean isNewDoc = mutation.isNew();

                        if (isNewDoc && logger.isTraceEnabled())
                            logger.trace("Adding new document {}", docId);

                        for (IndexEntry e : mutation.getAdditions()) {
                            final Object fieldValue = (e.value instanceof Geoshape)
                                                         ? GeoToWktConverter.convertToWktString((Geoshape) e.value)
                                                         : e.value;

                            doc.setField(e.field, isNewDoc
                                    ? fieldValue : new HashMap<String, Object>(1) {{ put("set", fieldValue); }});
                        }

                        if (isNewDoc && ttl > 0)
                            doc.setField(ttlField, String.format("+%dSECONDS", ttl));

                        changes.add(doc);
                    }
                }

                commitDeletes(coreName, deleteIds);
                commitDocumentChanges(coreName, changes);
            }
        } catch (Exception e) {
            throw storageException(e);
        }
    }

    @Override
    public void restore(Map<String, Map<String, List<IndexEntry>>> documents, KeyInformation.IndexRetriever informations, BaseTransaction tx) throws BackendException {
        try {
            for (Map.Entry<String, Map<String, List<IndexEntry>>> stores : documents.entrySet()) {
                final String coreName = stores.getKey();

                List<String> deleteIds = new ArrayList<String>();
                List<SolrInputDocument> newDocuments = new ArrayList<SolrInputDocument>();

                for (Map.Entry<String, List<IndexEntry>> entry : stores.getValue().entrySet()) {
                    final String docID = entry.getKey();
                    final List<IndexEntry> content = entry.getValue();

                    if (content == null || content.isEmpty()) {
                        if (logger.isTraceEnabled())
                            logger.trace("Deleting document [{}]", docID);

                        deleteIds.add(docID);
                        continue;
                    }

                    newDocuments.add(new SolrInputDocument() {{
                        setField(keyFieldIds.get(coreName), docID);

                        for (IndexEntry addition : content) {
                            Object fieldValue = addition.value;
                            setField(addition.field, (fieldValue instanceof Geoshape)
                                    ? GeoToWktConverter.convertToWktString((Geoshape) fieldValue)
                                    : fieldValue);
                        }
                    }});
                }

                commitDeletes(coreName, deleteIds);
                commitDocumentChanges(coreName, newDocuments);
            }
        } catch (Exception e) {
            throw new TemporaryBackendException("Could not restore Solr index", e);
        }
    }

    private void deleteIndividualFieldsFromIndex(String coreName, String keyIdField, String docId, HashSet<IndexEntry> fieldDeletions) throws SolrServerException, IOException {
        if (fieldDeletions.isEmpty())
            return;

        Map<String, String> fieldDeletes = new HashMap<String, String>(1) {{ put("set", null); }};

        SolrInputDocument doc = new SolrInputDocument();
        doc.addField(keyIdField, docId);
        StringBuilder sb = new StringBuilder();
        for (IndexEntry fieldToDelete : fieldDeletions) {
            doc.addField(fieldToDelete.field, fieldDeletes);
            sb.append(fieldToDelete).append(",");
        }

        if (logger.isTraceEnabled())
            logger.trace("Deleting individual fields [{}] for document {}", sb.toString(), docId);

        UpdateRequest singleDocument = newUpdateRequest(coreName);
        singleDocument.add(doc);
        solrServer.request(singleDocument);
    }

    private void commitDocumentChanges(String coreName, Collection<SolrInputDocument> documents) throws SolrServerException, IOException {
        if (documents.size() == 0)
            return;

        try {
            solrServer.request(newUpdateRequest(coreName).add(documents));
        } catch (HttpSolrServer.RemoteSolrException rse) {
            logger.error("Unable to save documents to Solr as one of the shape objects stored were not compatible with Solr.", rse);
            logger.error("Details in failed document batch: ");
            for (SolrInputDocument d : documents) {
                Collection<String> fieldNames = d.getFieldNames();
                for (String name : fieldNames) {
                    logger.error(name + ":" + d.getFieldValue(name).toString());
                }
            }

            throw rse;
        }
    }

    private void commitDeletes(String coreName, List<String> deleteIds) throws SolrServerException, IOException {
        if (deleteIds.size() == 0)
            return;

        solrServer.request(newUpdateRequest(coreName).deleteById(deleteIds));
    }

    @Override
    public List<String> query(IndexQuery query, KeyInformation.IndexRetriever informations, BaseTransaction tx) throws BackendException {
        List<String> result;
        String core = query.getStore();
        String keyIdField = keyFieldIds.get(core);
        SolrQuery solrQuery = newQuery(core);
        solrQuery = buildQuery(solrQuery, query.getCondition());
        if (!query.getOrder().isEmpty()) {
            List<IndexQuery.OrderEntry> orders = query.getOrder();
            for (IndexQuery.OrderEntry order1 : orders) {
                String item = order1.getKey();
                SolrQuery.ORDER order = order1.getOrder() == Order.ASC ? SolrQuery.ORDER.asc : SolrQuery.ORDER.desc;
                solrQuery.addSort(new SolrQuery.SortClause(item, order));
            }
        }
        solrQuery.setStart(0);
        if (query.hasLimit()) {
            solrQuery.setRows(query.getLimit());
        } else {
            solrQuery.setRows(maxResults);
        }
        try {
            QueryResponse response = solrServer.query(solrQuery);

            if (logger.isDebugEnabled())
                logger.debug("Executed query [{}] in {} ms", query.getCondition(), response.getElapsedTime());

            int totalHits = response.getResults().size();

            if (!query.hasLimit() && totalHits >= maxResults)
                logger.warn("Query result set truncated to first [{}] elements for query: {}", maxResults, query);

            result = new ArrayList<String>(totalHits);
            for (SolrDocument hit : response.getResults()) {
                result.add(hit.getFieldValue(keyIdField).toString());
            }
        } catch (HttpSolrServer.RemoteSolrException e) {
            logger.error("Query did not complete because parameters were not recognized : ", e);
            throw new PermanentBackendException(e);
        } catch (SolrServerException e) {
            logger.error("Unable to query Solr index.", e);
            throw new PermanentBackendException(e);
        }
        return result;
    }

    @Override
    public Iterable<RawQuery.Result<String>> query(RawQuery query, KeyInformation.IndexRetriever informations, BaseTransaction tx) throws BackendException {
        List<RawQuery.Result<String>> result;
        String core = query.getStore();
        String keyIdField = keyFieldIds.get(core);
        SolrQuery solrQuery = newQuery(core)
                                .addFilterQuery(query.getQuery())
                                .addField(keyIdField)
                                .addField("score")
                                .setStart(query.getOffset())
                                .setRows(query.hasLimit() ? query.getLimit() : maxResults);

        try {
            QueryResponse response = solrServer.query(solrQuery);
            if (logger.isDebugEnabled())
                logger.debug("Executed query [{}] in {} ms", query.getQuery(), response.getElapsedTime());

            int totalHits = response.getResults().size();
            if (!query.hasLimit() && totalHits >= maxResults) {
                logger.warn("Query result set truncated to first [{}] elements for query: {}", maxResults, query);
            }
            result = new ArrayList<RawQuery.Result<String>>(totalHits);

            for (SolrDocument hit : response.getResults()) {
                double score = Double.parseDouble(hit.getFieldValue("score").toString());
                result.add(new RawQuery.Result<String>(hit.getFieldValue(keyIdField).toString(), score));
            }
        } catch (HttpSolrServer.RemoteSolrException e) {
            logger.error("Query did not complete because parameters were not recognized : ", e);
            throw new PermanentBackendException(e);
        } catch (SolrServerException e) {
            logger.error("Unable to query Solr index.", e);
            throw new PermanentBackendException(e);
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
     * @throws com.thinkaurelius.titan.diskstorage.BackendException
     */
    @Override
    public BaseTransactionConfigurable beginTransaction(BaseTransactionConfig config) throws BackendException {
        return new DefaultTransaction(config);
    }

    @Override
    public void close() throws BackendException {
        logger.trace("Shutting down connection to Solr", solrServer);
        solrServer.shutdown();
        solrServer = null;
    }

    @Override
    public void clearStorage() throws BackendException {
        try {
            logger.trace("Clearing storage from Solr", solrServer);

            for (String core : cores) {
                UpdateRequest deleteAll = newUpdateRequest(core);
                deleteAll.deleteByQuery("*:*");

                solrServer.request(deleteAll);
            }
        } catch (SolrServerException e) {
            logger.error("Unable to clear storage from index due to server error on Solr.", e);
            throw new PermanentBackendException(e);
        } catch (IOException e) {
            logger.error("Unable to clear storage from index due to low-level I/O error.", e);
            throw new PermanentBackendException(e);
        } catch (Exception e) {
            logger.error("Unable to clear storage from index due to general error.", e);
            throw new PermanentBackendException(e);
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

    @Override
    public IndexFeatures getFeatures() {
        return SOLR_FEATURES;
    }

    private BackendException storageException(Exception solrException) {
        return new TemporaryBackendException("Unable to complete query on Solr.", solrException);
    }

    private static void createCollectionIfNotExists(CloudSolrServer server, Configuration config, String[] collections)
            throws IOException, SolrServerException, KeeperException, InterruptedException {
        for (String collection : collections) {
            if (!checkIfCollectionExists(server, collection)) {
                Integer numShards = config.get(NUM_SHARDS);
                Integer maxShardsPerNode = config.get(MAX_SHARDS_PER_NODE);
                Integer replicationFactor = config.get(REPLICATION_FACTOR);

                CollectionAdminRequest.Create createRequest = new CollectionAdminRequest.Create();

                createRequest.setConfigName(collection);
                createRequest.setCollectionName(collection);
                createRequest.setNumShards(numShards);
                createRequest.setMaxShardsPerNode(maxShardsPerNode);
                createRequest.setReplicationFactor(replicationFactor);

                CollectionAdminResponse createResponse = createRequest.process(server);
                if (createResponse.isSuccess()) {
                    logger.trace("Collection {} successfully created.", collection);
                } else {
                    throw new SolrServerException(Joiner.on("\n").join(createResponse.getErrorMessages()));
                }
            }

            waitForRecoveriesToFinish(server, collection);
        }
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
            logger.info("Exiting solr wait");
        }
    }

    private static UpdateRequest newUpdateRequest(String core) {
        UpdateRequest req = new UpdateRequest();
        req.setParam(CORE_PARAM, core);
        req.setAction(UpdateRequest.ACTION.COMMIT, true, true);
        return req;
    }
    private static SolrQuery newQuery(String core) {
        return new SolrQuery().setParam(CORE_PARAM, core).setQuery("*:*");
    }
}
