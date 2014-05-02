package com.thinkaurelius.titan.graphdb.fulgora;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.olap.OLAPJob;
import com.thinkaurelius.titan.core.olap.OLAPJobBuilder;
import com.thinkaurelius.titan.core.olap.OLAPQueryBuilder;
import com.thinkaurelius.titan.core.olap.StateInitializer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.internal.RelationCategory;
import com.thinkaurelius.titan.graphdb.query.BackendQueryHolder;
import com.thinkaurelius.titan.graphdb.query.vertex.AbstractVertexCentricQueryBuilder;
import com.thinkaurelius.titan.graphdb.query.vertex.BaseVertexCentricQuery;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Predicate;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class FulgoraBuilder<S> implements OLAPJobBuilder<S> {

    public static final String STATE_KEY = "state";
    public static final int DEFAULT_HARD_QUERY_LIMIT = 100000;

    private static final StateInitializer NULL_STATE = new StateInitializer() {
        @Override
        public Object initialState() {
            return null;
        }
    };

    private final StandardTitanGraph graph;
    private OLAPJob olapJob;
    private String stateKey;
    private StateInitializer<S> initializer;
    private Map<Long,S> initialState;
    private Map<String,Object> txOptions;
    private List<SliceQuery> queries;
    private int numProcessingThreads;
    private long numVertices;
    private int hardQueryLimit;

    public FulgoraBuilder(StandardTitanGraph graph) {
        Preconditions.checkArgument(graph!=null);
        this.graph=graph;
        this.stateKey = STATE_KEY;
        this.initializer = NULL_STATE;
        this.initialState = null;
        this.txOptions = new HashMap<String,Object>();
        this.queries = new ArrayList<SliceQuery>();
        this.numProcessingThreads = 2;
        this.numVertices = -1;
        this.hardQueryLimit = DEFAULT_HARD_QUERY_LIMIT;
    }

    @Override
    public FulgoraBuilder<S> setJob(OLAPJob job) {
        Preconditions.checkArgument(job!=null);
        olapJob = job;
        return this;
    }

    @Override
    public FulgoraBuilder<S> setStateKey(String stateKey) {
        Preconditions.checkArgument(StringUtils.isNotBlank(stateKey));
        this.stateKey=stateKey;
        return this;
    }

    @Override
    public FulgoraBuilder<S> setInitializer(StateInitializer<S> initial) {
        Preconditions.checkNotNull(initial);
        this.initializer=initial;
        return this;
    }

    @Override
    public FulgoraBuilder<S> setInitialState(Map<Long,S> values) {
        Preconditions.checkArgument(values!=null);
        this.initialState=values;
        return this;
    }

    @Override
    public FulgoraBuilder<S> setNumVertices(long numVertices) {
        Preconditions.checkArgument(numVertices>0 && numVertices<Integer.MAX_VALUE,
                "Invalid value for number of vertices: %s",numVertices);
        this.numVertices=numVertices;
        return this;
    }

    @Override
    public FulgoraBuilder<S> setNumProcessingThreads(int numThreads) {
        Preconditions.checkArgument(numThreads>0,
                "Need to specify a positive number of processing threads: %s",numThreads);
        this.numProcessingThreads = numThreads;
        return this;
    }

    public FulgoraBuilder<S> setCustomTxOptions(String key, Object value) {
        Preconditions.checkArgument(StringUtils.isNotBlank(key) && value!=null);
        txOptions.put(key,value);
        return this;
    }

    @Override
    public OLAPQueryBuilder addQuery() {
        return new QueryBuilder((StandardTitanTx)graph.buildTransaction().readOnly().setVertexCacheSize(100).start());
    }

    @Override
    public Future<Map<Long,S>> execute() {
        Preconditions.checkArgument(!queries.isEmpty(),"Need to register at least one query");
        TransactionBuilder txBuilder = graph.buildTransaction().readOnly().setVertexCacheSize(100);
        for (Map.Entry<String,Object> txOption : txOptions.entrySet())
            txBuilder.setCustomOption(txOption.getKey(),txOption.getValue());
        final StandardTitanTx tx = (StandardTitanTx)txBuilder.start();
        FulgoraExecutor executor = new FulgoraExecutor(queries,tx,(int)numVertices,numProcessingThreads,
                stateKey, olapJob, initializer,initialState);
        new Thread(executor).start();
        return executor;
    }

    private class QueryBuilder extends AbstractVertexCentricQueryBuilder implements OLAPQueryBuilder<S> {

        private final StandardTitanTx tx;

        public QueryBuilder(StandardTitanTx tx) {
            super(tx);
            this.tx=tx;
        }

        private void relations(RelationCategory returnType) {
            BaseVertexCentricQuery vq = super.constructQuery(returnType);
            for (int i = 0; i < vq.numSubQueries(); i++) {
                BackendQueryHolder<SliceQuery> bq = vq.getSubQuery(i);
                SliceQuery sq = bq.getBackendQuery();
                queries.add(sq.updateLimit(bq.isFitted()?vq.getLimit():hardQueryLimit));
            }
            tx.rollback();
        }

        @Override
        public OLAPJobBuilder edges() {
            relations(RelationCategory.EDGE);
            return FulgoraBuilder.this;
        }

        @Override
        public OLAPJobBuilder properties() {
            relations(RelationCategory.PROPERTY);
            return FulgoraBuilder.this;
        }

        @Override
        public OLAPJobBuilder relations() {
            relations(RelationCategory.RELATION);
            return FulgoraBuilder.this;
        }

        /*
        ########### SIMPLE OVERWRITES ##########
         */

        @Override
        public QueryBuilder has(TitanKey key, Object value) {
            super.has(key, value);
            return this;
        }

        @Override
        public QueryBuilder has(TitanLabel label, TitanVertex vertex) {
            super.has(label, vertex);
            return this;
        }

        @Override
        public QueryBuilder has(String type, Object value) {
            super.has(type, value);
            return this;
        }

        @Override
        public QueryBuilder hasNot(String key, Object value) {
            super.hasNot(key, value);
            return this;
        }

        @Override
        public QueryBuilder has(String key, Predicate predicate, Object value) {
            super.has(key, predicate, value);
            return this;
        }

        @Override
        public QueryBuilder has(TitanKey key, Predicate predicate, Object value) {
            super.has(key, predicate, value);
            return this;
        }

        @Override
        public QueryBuilder has(String key) {
            super.has(key);
            return this;
        }

        @Override
        public QueryBuilder hasNot(String key) {
            super.hasNot(key);
            return this;
        }

        @Override
        public <T extends Comparable<?>> QueryBuilder interval(TitanKey key, T start, T end) {
            super.interval(key, start, end);
            return this;
        }

        @Override
        public <T extends Comparable<?>> QueryBuilder interval(String key, T start, T end) {
            super.interval(key, start, end);
            return this;
        }

        @Override
        public QueryBuilder types(TitanType... types) {
            super.types(types);
            return this;
        }

        @Override
        public QueryBuilder labels(String... labels) {
            super.labels(labels);
            return this;
        }

        @Override
        public QueryBuilder keys(String... keys) {
            super.keys(keys);
            return this;
        }

        public QueryBuilder type(TitanType type) {
            super.type(type);
            return this;
        }

        @Override
        public QueryBuilder direction(Direction d) {
            super.direction(d);
            return this;
        }

        @Override
        public QueryBuilder limit(int limit) {
            super.limit(limit);
            return this;
        }


    }

}
