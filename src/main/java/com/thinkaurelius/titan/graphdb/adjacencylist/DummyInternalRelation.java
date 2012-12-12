package com.thinkaurelius.titan.graphdb.adjacencylist;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.query.AtomicQuery;
import com.thinkaurelius.titan.graphdb.relations.InternalRelation;
import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;
import com.thinkaurelius.titan.graphdb.vertices.InternalTitanVertex;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import java.util.Set;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */
class TypeInternalRelation extends DummyInternalRelation {
    
    private final TitanType type;
    
    TypeInternalRelation(TitanType type, boolean lowerBound) {
        super(lowerBound);
        Preconditions.checkNotNull(type);
        this.type=type;
    }

    @Override
    int compareTo(InternalRelation r) {
        Preconditions.checkNotNull(r);
        if (r instanceof DummyInternalRelation) {
            Preconditions.checkArgument(r instanceof TypeInternalRelation && ((TypeInternalRelation)r).type.equals(type));
            return compareOnEquals()-((DummyInternalRelation)r).compareOnEquals();
        } else {
            if (type.equals(r.getType())) return compareOnEquals();
            else return TypeComparator.INSTANCE.compare(type,r.getType());
        }
    }
}

class GroupInternalRelation extends DummyInternalRelation {

    private final TypeGroup group;

    GroupInternalRelation(TypeGroup group, boolean lowerBound) {
        super(lowerBound);
        Preconditions.checkNotNull(group);
        this.group=group;
    }

    @Override
    int compareTo(InternalRelation r) {
        Preconditions.checkNotNull(r);
        if (r instanceof DummyInternalRelation) {
            Preconditions.checkArgument(r instanceof GroupInternalRelation && ((GroupInternalRelation)r).group.equals(group));
            return compareOnEquals()-((DummyInternalRelation)r).compareOnEquals();
        } else {
            TypeGroup g = r.getType().getGroup();
            if (group.equals(g)) return compareOnEquals();
            else return TypeComparator.INSTANCE.compareGroups(group,g);
        }
    }
}


abstract class DummyInternalRelation implements InternalRelation {

    private int compareOnEquals;

    DummyInternalRelation(boolean lowerBound) {
        if (lowerBound) makeLowerBound();
        else makeUpperBound();
    }
    
    void makeLowerBound() {
        compareOnEquals=-1;
    }

    void makeUpperBound() {
        compareOnEquals=1;
    }
    
    int compareOnEquals() {
        return compareOnEquals;
    }
    
    abstract int compareTo(InternalRelation r);

    @Override
    public InternalTitanVertex getVertex(int pos) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getArity() {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void forceDelete() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isHidden() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isInline() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public InternalTitanTransaction getTransaction() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Iterable<InternalRelation> getRelations(AtomicQuery query, boolean loadRemaining) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void removeRelation(InternalRelation e) {
    }

    @Override
    public boolean addRelation(InternalRelation e, boolean isNew) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void loadedEdges(AtomicQuery query) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean hasLoadedEdges(AtomicQuery query) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setID(long id) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public TitanType getType() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Direction getDirection(TitanVertex vertex) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isIncidentOn(TitanVertex vertex) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isDirected() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isUndirected() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isUnidirected() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isModifiable() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isSimple() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isLoop() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isProperty() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isEdge() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public TitanEdge addEdge(TitanLabel label, TitanVertex vertex) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public TitanEdge addEdge(String label, TitanVertex vertex) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public TitanProperty addProperty(TitanKey key, Object attribute) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public TitanProperty addProperty(String key, Object attribute) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setProperty(String key, Object value) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Object removeProperty(String key) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Object getId() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public TitanQuery query() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Object getProperty(TitanKey key) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Object getProperty(String key) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Set<String> getPropertyKeys() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public <O> O getProperty(TitanKey key, Class<O> clazz) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public <O> O getProperty(String key, Class<O> clazz) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Iterable<TitanProperty> getProperties() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Iterable<TitanProperty> getProperties(TitanKey key) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Iterable<TitanProperty> getProperties(String key) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Iterable<TitanEdge> getTitanEdges(Direction d, TitanLabel... labels) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Iterable<Edge> getEdges(Direction d, String... labels) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String... strings) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Iterable<TitanEdge> getEdges() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Iterable<TitanRelation> getRelations() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public long getEdgeCount() {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public long getPropertyCount() {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isConnected() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public long getID() {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean hasID() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void remove() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isNew() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isLoaded() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isModified() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isRemoved() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isReferenceVertex() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isAvailable() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isAccessible() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
