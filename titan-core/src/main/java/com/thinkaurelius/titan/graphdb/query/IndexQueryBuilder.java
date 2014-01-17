package com.thinkaurelius.titan.graphdb.query;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.thinkaurelius.titan.core.Parameter;
import com.thinkaurelius.titan.core.TitanElement;
import com.thinkaurelius.titan.core.TitanIndexQuery;
import com.thinkaurelius.titan.diskstorage.indexing.RawQuery;
import com.thinkaurelius.titan.graphdb.database.IndexSerializer;
import com.thinkaurelius.titan.graphdb.internal.ElementType;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.List;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class IndexQueryBuilder extends BaseQuery implements TitanIndexQuery {

    private static final Logger log = LoggerFactory.getLogger(IndexQueryBuilder.class);


    private static final String VERTEX_PREFIX = "v.";
    private static final String EDGE_PREFIX = "e.";

    private final StandardTitanTx tx;
    private final IndexSerializer serializer;

    private String indexName;
    private String query;
    private final List<Parameter> parameters;

    private String prefix;
    private final String unkownKeyName;


    public IndexQueryBuilder(StandardTitanTx tx, IndexSerializer serializer) {
        Preconditions.checkNotNull(tx);
        Preconditions.checkNotNull(serializer);
        this.tx = tx;
        this.serializer = serializer;

        parameters = Lists.newArrayList();
        unkownKeyName = tx.getGraph().getConfiguration().getUnknownIndexKeydName();
    }

    //################################################
    // Inspection Methods
    //################################################

    public String getIndex() {
        return indexName;
    }

    public Parameter[] getParameters() {
        return parameters.toArray(new Parameter[parameters.size()]);
    }

    public String getQuery() {
        return query;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getUnknownKeyName() {
        return unkownKeyName;
    }


    //################################################
    // Builder Methods
    //################################################

    public IndexQueryBuilder setIndex(String indexName) {
        Preconditions.checkArgument(StringUtils.isNotBlank(indexName));
        this.indexName=indexName;
        return this;
    }

    public IndexQueryBuilder setQuery(String query) {
        Preconditions.checkArgument(StringUtils.isNotBlank(query));
        this.query=query;
        return this;
    }

    @Override
    public IndexQueryBuilder limit(int limit) {
        super.setLimit(limit);
        return this;
    }

    @Override
    public IndexQueryBuilder addParameter(Parameter para) {
        parameters.add(para);
        return this;
    }

    @Override
    public IndexQueryBuilder addParameters(Iterable<Parameter> paras) {
        Iterables.addAll(parameters, paras);
        return this;
    }

    @Override
    public IndexQueryBuilder addParameters(Parameter... paras) {
        for (Parameter para: paras) addParameter(para);
        return this;
    }

    private Iterable<Result<TitanElement>> execute(ElementType resultType) {
        Preconditions.checkNotNull(indexName);
        Preconditions.checkNotNull(query);
        if (tx.hasModifications())
            log.warn("Modifications in this transaction might not be accurately reflected in this index query: {}",query);
        Iterable<RawQuery.Result> result = serializer.executeQuery(this,resultType,tx.getTxHandle(),tx);
        final Function<Object, ? extends TitanElement> conversionFct = tx.getConversionFunction(resultType);
        return Iterables.filter(Iterables.transform(result, new Function<RawQuery.Result, Result<TitanElement>>() {
            @Nullable
            @Override
            public Result<TitanElement> apply(@Nullable RawQuery.Result result) {
                return new ResultImpl<TitanElement>(conversionFct.apply(result.getResult()),result.getScore());
            }
        }),new Predicate<Result<TitanElement>>() {
            @Override
            public boolean apply(@Nullable Result<TitanElement> r) {
                return !r.getElement().isRemoved();
            }
        });
    }

    @Override
    public Iterable<Result<Vertex>> vertices() {
        setPrefix(VERTEX_PREFIX);
        return (Iterable)execute(ElementType.VERTEX);
    }

    @Override
    public Iterable<Result<Edge>> edges() {
        setPrefix(EDGE_PREFIX);
        return (Iterable)execute(ElementType.EDGE);
    }

    private void setPrefix(String prefix) {
        Preconditions.checkArgument(StringUtils.isNotBlank(prefix));
        if (this.prefix==null) this.prefix=prefix;
    }

    private static class ResultImpl<V extends Element> implements Result<V> {

        private final V element;
        private final double score;

        private ResultImpl(V element, double score) {
            this.element = element;
            this.score = score;
        }

        @Override
        public V getElement() {
            return element;
        }

        @Override
        public double getScore() {
            return score;
        }
    }
}
