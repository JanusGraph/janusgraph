package com.thinkaurelius.titan.graphdb.fulgora;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.olap.*;
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
    private static final int NUM_VERTEX_DEFAULT = 10000;

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
    private FulgoraResult<S> initialState;
    private Map<String,Object> txOptions;
    private Map<String,FulgoraRelationQuery> queries;
    private int numProcessingThreads;
    private int numVertices;
    private int hardQueryLimit;

    public FulgoraBuilder(StandardTitanGraph graph) {
        Preconditions.checkArgument(graph!=null);
        this.graph=graph;
        this.stateKey = STATE_KEY;
        this.initializer = NULL_STATE;
        this.initialState = null;
        this.txOptions = new HashMap<String,Object>();
        this.queries = new HashMap<String, FulgoraRelationQuery>();
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
        this.initialState=new FulgoraResult<S>(values,graph.getIDManager());
        return this;
    }

    @Override
    public OLAPJobBuilder<S> setInitialState(OLAPResult<S> values) {
        Preconditions.checkArgument(values!=null && values instanceof FulgoraResult,"Invalid result set provided: " + values);
        this.initialState = (FulgoraResult)values;
        return this;
    }

    @Override
    public FulgoraBuilder<S> setNumVertices(long numVertices) {
        Preconditions.checkArgument(numVertices>0 && numVertices<Integer.MAX_VALUE,
                "Invalid value for number of vertices: %s",numVertices);
        this.numVertices=(int)numVertices;
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
    public Future<OLAPResult<S>> execute() {
        Preconditions.checkArgument(!queries.isEmpty(),"Need to register at least one query");
        TransactionBuilder txBuilder = graph.buildTransaction().readOnly().setVertexCacheSize(100);
        for (Map.Entry<String,Object> txOption : txOptions.entrySet())
            txBuilder.setCustomOption(txOption.getKey(),txOption.getValue());
        final StandardTitanTx tx = (StandardTitanTx)txBuilder.start();
        FulgoraResult<S> state = initialState!=null?initialState:new FulgoraResult<S>(numVertices>=0?numVertices:NUM_VERTEX_DEFAULT,graph.getIDManager());
        FulgoraExecutor executor = new FulgoraExecutor(queries,tx,graph.getIDManager(),
                numProcessingThreads, stateKey, olapJob, initializer, state);
        new Thread(executor).start();
        return executor;
    }

    private class QueryBuilder<M> extends AbstractVertexCentricQueryBuilder<QueryBuilder<M>> implements OLAPQueryBuilder<S,M,QueryBuilder<M>> {

        private final StandardTitanTx tx;
        private String name = null;

        public QueryBuilder(StandardTitanTx tx) {
            super(tx);
            this.tx=tx;
        }

        private List<SliceQuery> relations(RelationCategory returnType) {
            if (name==null) {
                if (hasSingleType()) name = getSingleType().getName();
                else throw new IllegalStateException("Need to specify an explicit name for this query");
            }
            try {
                BaseVertexCentricQuery vq = super.constructQuery(returnType);
                List<SliceQuery> queries = new ArrayList<SliceQuery>(vq.numSubQueries());
                for (int i = 0; i < vq.numSubQueries(); i++) {
                    BackendQueryHolder<SliceQuery> bq = vq.getSubQuery(i);
                    SliceQuery sq = bq.getBackendQuery();
                    queries.add(sq.updateLimit(bq.isFitted() ? vq.getLimit() : hardQueryLimit));
                }
                return queries;
            } finally {
                tx.rollback();
            }
        }

        @Override
        protected QueryBuilder getThis() {
            return this;
        }

        @Override
        public QueryBuilder setName(String name) {
            Preconditions.checkArgument(StringUtils.isNotBlank(name),"Invalid name provided: %s",name);
            this.name=name;
            return getThis();
        }

        @Override
        public<M> FulgoraBuilder<S> edges(Gather<S,M> gather, Combiner<M> combiner) {
            List<SliceQuery> qs = relations(RelationCategory.EDGE);
            synchronized (queries) {
                Preconditions.checkArgument(!queries.containsKey(name),"Name already in use: %s",name);
                queries.put(name,new FulgoraEdgeQuery(qs,gather,combiner));
            }
            return FulgoraBuilder.this;
        }

        @Override
        public<M> FulgoraBuilder<S> properties(Function<TitanProperty,M> gather, Combiner<M> combiner) {
            List<SliceQuery> qs = relations(RelationCategory.PROPERTY);
            synchronized (queries) {
                Preconditions.checkArgument(!queries.containsKey(name),"Name already in use: %s",name);
                queries.put(name,new FulgoraPropertyQuery(qs,gather,combiner));
            }

            return FulgoraBuilder.this;
        }

        @Override
        public FulgoraBuilder<S> edges(Combiner<S> combiner) {
            return edges(FulgoraEdgeQuery.<S>getDefaultGather(),combiner);
        }

        @Override
        public FulgoraBuilder<S> properties(Combiner<Object> combiner) {
            return properties(FulgoraPropertyQuery.SINGLE_VALUE_GATHER,combiner);
        }

        @Override
        public FulgoraBuilder<S> properties() {
            if (hasSingleType()) {
                RelationType type = getSingleType();
                Preconditions.checkArgument(type instanceof PropertyKey);
                if (((PropertyKey)type).getCardinality()==Cardinality.SINGLE) {
                    return properties(FulgoraPropertyQuery.SINGLE_VALUE_GATHER,FulgoraPropertyQuery.SINGLE_COMBINER);
                }
            }
            return properties(FulgoraPropertyQuery.VALUE_LIST_GATHER,FulgoraPropertyQuery.VALUE_LIST_COMBINER);
        }

        /*
        ########### SIMPLE OVERWRITES ##########
         */

        @Override
        public QueryBuilder has(PropertyKey key, Object value) {
            super.has(key, value);
            return this;
        }

        @Override
        public QueryBuilder has(EdgeLabel label, TitanVertex vertex) {
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
        public QueryBuilder has(PropertyKey key, Predicate predicate, Object value) {
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
        public <T extends Comparable<?>> QueryBuilder interval(PropertyKey key, T start, T end) {
            super.interval(key, start, end);
            return this;
        }

        @Override
        public <T extends Comparable<?>> QueryBuilder interval(String key, T start, T end) {
            super.interval(key, start, end);
            return this;
        }

        @Override
        public QueryBuilder types(RelationType... types) {
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

        public QueryBuilder type(RelationType type) {
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
