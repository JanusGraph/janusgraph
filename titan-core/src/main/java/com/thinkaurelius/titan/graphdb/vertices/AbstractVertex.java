package com.thinkaurelius.titan.graphdb.vertices;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.graphdb.internal.AbstractElement;
import com.thinkaurelius.titan.graphdb.internal.ElementLifeCycle;
import com.thinkaurelius.titan.graphdb.internal.InternalRelationType;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.query.vertex.VertexCentricQueryBuilder;
import com.thinkaurelius.titan.graphdb.relations.SimpleTitanProperty;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.VertexLabelVertex;
import com.thinkaurelius.titan.graphdb.types.system.BaseKey;
import com.thinkaurelius.titan.graphdb.types.system.BaseLabel;
import com.thinkaurelius.titan.graphdb.types.system.BaseVertexLabel;
import com.thinkaurelius.titan.graphdb.util.ElementHelper;
import com.tinkerpop.gremlin.structure.*;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.util.StreamFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.stream.Stream;

public abstract class AbstractVertex extends AbstractElement implements InternalVertex, Vertex.Iterators {

    private final StandardTitanTx tx;


    protected AbstractVertex(StandardTitanTx tx, long id) {
        super(id);
        assert tx != null;
        this.tx = tx;
    }

    @Override
    public final InternalVertex it() {
        if (tx.isOpen())
            return this;

        InternalVertex next = (InternalVertex) tx.getNextTx().getVertex(longId());
        if (next == null) throw InvalidElementException.removedException(this);
        else return next;
    }

    @Override
    public final StandardTitanTx tx() {
        return tx.isOpen() ? tx : tx.getNextTx();
    }

    @Override
    public long getCompareId() {
        if (tx.isPartitionedVertex(this)) return tx.getIdInspector().getCanonicalVertexId(longId());
        else return longId();
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

    @Override
    public Object id() {
        return longId();
    }

    @Override
    public boolean isModified() {
        return ElementLifeCycle.isModified(it().getLifeCycle());
    }




	/* ---------------------------------------------------------------
     * Changing Edges
	 * ---------------------------------------------------------------
	 */

    @Override
    public synchronized void remove() {
        if (isRemoved()) throw InvalidElementException.removedException(this);
        Iterator<TitanRelation> iter = it().query().noPartitionRestriction().relations().iterator();
        while (iter.hasNext()) {
            iter.next();
            iter.remove();
        }
        //Remove all system types on the vertex
        for (TitanRelation r : it().query().noPartitionRestriction().system().relations()) {
            RelationType t = r.getType();
            assert t==BaseLabel.VertexLabelEdge || t==BaseKey.VertexExists;
            r.remove();
        }
    }

	/* ---------------------------------------------------------------
	 * TitanRelation Iteration/Access
	 * ---------------------------------------------------------------
	 */

    @Override
    public String label() {
        return vertexLabel().name();
    }

    protected Vertex getVertexLabelInternal() {
        return Iterables.getOnlyElement(tx().query(this).noPartitionRestriction().type(BaseLabel.VertexLabelEdge).direction(Direction.OUT).vertices(),null);
    }

    @Override
    public VertexLabel vertexLabel() {
        Vertex label = getVertexLabelInternal();
        if (label==null) return BaseVertexLabel.DEFAULT_VERTEXLABEL;
        else return (VertexLabelVertex)label;
    }

    @Override
    public VertexCentricQueryBuilder query() {
        Preconditions.checkArgument(!isRemoved(), "Cannot access a removed vertex: %s", this);
        return tx().query(this);
    }

    @Override
    public <O> O value(PropertyKey key) {
        if (!((InternalRelationType)key).isInvisibleType() && tx().getConfiguration().hasPropertyPrefetching()) {
            properties().count().next();
        }
        Iterator<TitanVertexProperty> iter = query().type(key).properties().iterator();
        if (key.cardinality()== Cardinality.SINGLE) {
            if (iter.hasNext()) return (O)iter.next().value();
            else return null;
        } else {
            List<Object> result = new ArrayList<Object>();
            while (iter.hasNext()) {
                result.add(iter.next().value());
            }
            return (O)result;
        }
    }

    @Override
    public <O> O value(String key) {
        if (!tx().containsRelationType(key)) return null;
        else return value(tx().getPropertyKey(key));
    }

	/* ---------------------------------------------------------------
	 * Convenience Methods for TitanElement Creation
	 * ---------------------------------------------------------------
	 */

    @Override
    public<V> TitanVertexProperty<V> property(String key, V value) {
        return tx().addProperty(it(), tx().getOrCreatePropertyKey(key), value);
    }

    @Override
    public <V> TitanVertexProperty<V> singleProperty(String key, V value, Object... keyValues) {
        TitanVertexProperty<V> p = tx().setProperty(it(),tx().getOrCreatePropertyKey(key),value);
        ElementHelper.attachProperties(p,keyValues);
        return p;
    }

    @Override
    public TitanEdge addEdge(String label, Vertex vertex, Object... keyValues) {
        Preconditions.checkArgument(vertex instanceof TitanVertex,"Invalid vertex provided: %s",vertex);
        TitanEdge edge = tx().addEdge(it(), (TitanVertex) vertex, tx().getOrCreateEdgeLabel(label));
        ElementHelper.attachProperties(edge,keyValues);
        return edge;
    }

	/* ---------------------------------------------------------------
	 * TinkPop Iterators Method
	 * ---------------------------------------------------------------
	 */

    @Override
    public Vertex.Iterators iterators() {
        return this;
    }

    @Override
    public Iterator<Edge> edgeIterator(Direction direction, int i, String... strings) {
        return (Iterator)query().direction(direction).limit(i).labels(strings).edges().iterator();
    }

    @Override
    public Iterator<Vertex> vertexIterator(Direction direction, int i, String... strings) {
        return (Iterator)query().direction(direction).limit(i).labels(strings).vertices().iterator();
    }

    public <V> Iterator<VertexProperty<V>> propertyIterator(boolean hidden, String... strings) {
        if (strings==null) strings=new String[0];
        return (Iterator)com.google.common.collect.Iterators.filter(query().keys(strings).properties().iterator(),
                new Predicate<TitanVertexProperty>() {
                    @Override
                    public boolean apply(@Nullable TitanVertexProperty prop) {
                        return hidden ^ !prop.isHidden();
                    }
                });
    }

    @Override
    public <V> Iterator<VertexProperty<V>> propertyIterator(String... strings) {
        return propertyIterator(false,strings);
    }

    @Override
    public <V> Iterator<VertexProperty<V>> hiddenPropertyIterator(String... strings) {
        return propertyIterator(true,strings);
    }

}
