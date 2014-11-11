package com.thinkaurelius.titan.graphdb.olap.job;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.RelationType;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.schema.TitanIndex;
import com.thinkaurelius.titan.diskstorage.configuration.BasicConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigNamespace;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.ScanMetrics;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.database.management.ManagementSystem;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class IndexUpdateJob {

    protected static final Logger log =
            LoggerFactory.getLogger(IndexRepairJob.class);

    protected static final String SUCCESS_TX = "success-tx";
    protected static final String FAILED_TX = "failed-tx";

    public static ConfigNamespace INDEX_JOB_NS = new ConfigNamespace(GraphDatabaseConfiguration.JOB_NS,"index","Configuration options relating to index jobs");

    public static final ConfigOption<String> INDEX_NAME = new ConfigOption<String>(INDEX_JOB_NS,"index-name",
            "The name of the index to be repaired. For vertex-centric indexes this is the name of " +
                    "the edge label or property key on which the index is installed.",
            ConfigOption.Type.LOCAL, String.class);

    public static final ConfigOption<String> INDEX_RELATION_TYPE = new ConfigOption<String>(INDEX_JOB_NS,"relation-type",
            "For a vertex-centric index, this is the name of the index associated with the " +
                    "relation type configured under index-name. This should remain empty for global graph indexes.",
            ConfigOption.Type.LOCAL, "");


    protected StandardTitanGraph graph;
    protected ManagementSystem mgmt = null;
    protected String indexName;
    protected String indexRelationTypeName;

    protected final boolean graphProvided;

    protected TitanIndex index;
    protected RelationType indexRelationType;


    public IndexUpdateJob() {
        graphProvided = false;
    }

    public IndexUpdateJob(final TitanGraph graph, final String indexName, final String indexRelationTypeName) {
        this.graph = (StandardTitanGraph)graph;
        this.indexName = indexName;
        this.indexRelationTypeName = indexRelationTypeName;
        graphProvided = true;
    }

    public boolean isGlobalGraphIndex() {
        return indexRelationTypeName ==null || StringUtils.isBlank(indexRelationTypeName);
    }

    public boolean isRelationTypeIndex() {
        return !isGlobalGraphIndex();
    }

    public void setup(Configuration config, ScanMetrics metrics) {
        if (!graphProvided) {
            Preconditions.checkArgument(config.has(INDEX_NAME), "Need to configure the name of the index to be repaired");
            this.indexName = config.get(INDEX_NAME);
            this.indexRelationTypeName = config.get(INDEX_RELATION_TYPE);
            log.info("Read index information: name={} type={}", indexName, indexRelationTypeName);
        }

        try {
            if (!graphProvided) this.graph = (StandardTitanGraph) TitanFactory.open((BasicConfiguration) config);
            this.mgmt = (ManagementSystem)graph.openManagement();

            if (isGlobalGraphIndex()) {
                index = mgmt.getGraphIndex(indexName);
            } else {
                indexRelationType = mgmt.getRelationType(indexRelationTypeName);
                Preconditions.checkArgument(indexRelationType!=null,"Could not find relation type: %s", indexRelationTypeName);
                index = mgmt.getRelationIndex(indexRelationType,indexName);
            }
            Preconditions.checkArgument(index!=null,"Could not find index: %s [%s]",indexName,indexRelationTypeName);
            log.info("Found index {}", indexName);
            validateIndexStatus();
        } catch (final Exception e) {
            if (null != mgmt && mgmt.isOpen())
                mgmt.rollback();
            if (!graphProvided && null != graph && graph.isOpen())
                graph.close();
            metrics.incrementCustom(FAILED_TX);
            throw new TitanException(e.getMessage(), e);
        }
    }

    public void teardown(ScanMetrics metrics) {
        try {
            if (null != mgmt && mgmt.isOpen())
                mgmt.commit();
            metrics.incrementCustom(SUCCESS_TX);
        } catch (RuntimeException e) {
            log.error("Transaction commit threw runtime exception:", e);
            metrics.incrementCustom(FAILED_TX);
            throw e;
        } finally {
            try {
                if (!graphProvided && null != graph && graph.isOpen())
                    graph.close();
            } catch (RuntimeException ex) {
                log.error("Could not close graph:",ex);
            }
        }
    }

    protected abstract void validateIndexStatus();


}
