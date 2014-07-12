package com.thinkaurelius.titan.hadoop;

import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.elasticsearch.common.Preconditions;
import org.elasticsearch.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.schema.SchemaStatus;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.diskstorage.configuration.BasicConfiguration;
import com.thinkaurelius.titan.hadoop.compat.HadoopCompat;
import com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader;
import com.thinkaurelius.titan.hadoop.config.ConfigurationUtil;
import com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration;

/**
 * Given (1) an InputFormat that iterates a Titan edgestore and converts each
 * row into a HadoopVertex and (2) the name of an already-defined index in
 * either the REGISTERED or ENABLED state, consider each HadoopVertex and
 * rebuild the named index accordingly. The index is written through a
 * TitanGraph instance. The KEYOUT and VALUEOUT type parameters are NullWritable
 * because this Mapper produces no conventional SequenceFile output. There's
 * nothing to combine or reduce since the writes go through TitanGraph and its
 * non-Hadoop backend interface.
 */
public class TitanIndexRepairMapper extends Mapper<NullWritable, FaunusVertex, NullWritable, NullWritable> {

    private static final Logger log =
            LoggerFactory.getLogger(TitanIndexRepairMapper.class);

    private static final HadoopCompat COMPAT = HadoopCompatLoader.getDefaultCompat();

    private static final EnumSet<SchemaStatus> ACCEPTED_INDEX_STATUSES =
            EnumSet.of(SchemaStatus.ENABLED, SchemaStatus.INSTALLED);

    private TitanGraph titanGraph;
    private String indexName;
    private String indexType;
    private boolean atLeastOneValidKey;
    private int keysChecked;

    public enum Counters {
        SUCCESSFUL_TRANSACTIONS,
        FAILED_TRANSACTIONS,
        SUCCESSFUL_GRAPH_SHUTDOWNS,
        FAILED_GRAPH_SHUTDOWNS,
    }

    @Override
    public void setup(
            final Mapper<NullWritable, FaunusVertex, NullWritable, NullWritable>.Context context) {
        Configuration hadoopConf = COMPAT.getContextConfiguration(context);
        BasicConfiguration titanConf = ConfigurationUtil.extractOutputConfiguration(hadoopConf);
        indexName = ConfigurationUtil.get(hadoopConf, TitanHadoopConfiguration.INDEX_NAME);
        indexType = ConfigurationUtil.get(hadoopConf, TitanHadoopConfiguration.INDEX_TYPE);
        Preconditions.checkNotNull(indexName);
        //Preconditions.checkNotNull(indexType); // is this true?
        log.info("Read index information: name={} type={}", indexName, indexType);
        titanGraph = TitanFactory.open(titanConf);
        log.info("Opened graph {}", titanGraph);
        validateIndexStatus();
    }

    @Override
    public void cleanup(final Mapper<NullWritable, FaunusVertex, NullWritable, NullWritable>.Context context) {
        try {
            titanGraph.commit();
            COMPAT.incrementContextCounter(context, Counters.SUCCESSFUL_TRANSACTIONS, 1L);
        } catch (RuntimeException e) {
            log.error("Transaction commit threw runtime exception:", e);
            COMPAT.incrementContextCounter(context, Counters.FAILED_TRANSACTIONS, 1L);
            throw e;
        }

        try {
            titanGraph.shutdown();
            COMPAT.incrementContextCounter(context, Counters.SUCCESSFUL_GRAPH_SHUTDOWNS, 1L);
        } catch (RuntimeException e) {
            log.error("Graph shutdown threw runtime exception:", e);
            COMPAT.incrementContextCounter(context, Counters.FAILED_GRAPH_SHUTDOWNS, 1L);
            throw e;
        }
    }

    @Override
    public void map(
            final NullWritable key, // ignored
            final FaunusVertex value,
            final Mapper<NullWritable, FaunusVertex, NullWritable, NullWritable>.Context context) {
        if (!atLeastOneValidKey) {
            String legalStates = Joiner.on(", ").join(ACCEPTED_INDEX_STATUSES);
            throw new IllegalStateException("At least one of the "
                    + keysChecked + " property keys associated with index "
                    + indexName + " must be in one of the following states: "
                    + legalStates);
        }
    }

    /**
     * Check that our target index is in either the ENABLED or REGISTERED state.
     */
    private void validateIndexStatus() {
        TitanManagement m = titanGraph.getManagementSystem();
        TitanGraphIndex idx = m.getGraphIndex(indexName);
        m.containsRelationType(indexType);
        if (null != idx) {
            log.info("Found index {}", indexName);
            for (PropertyKey key : idx.getFieldKeys()) {
                final SchemaStatus s = idx.getIndexStatus(key);
                final boolean b = ACCEPTED_INDEX_STATUSES.contains(s);
                atLeastOneValidKey |= b;
                keysChecked++;
                log.debug("Key {} has status {} (accepted={})", key, s, b);
            }
            log.info("Checking {} keys on index {}", keysChecked, indexName);
        } else {
            log.warn("Unknown index {}", indexName);
        }
        // TODO consider retrieving the current Job object and calling killJob() if !atLeastOneValidKey -- would be more efficient than throwing an exception on the first pair processed by each mapper
        m.rollback();
    }
}
