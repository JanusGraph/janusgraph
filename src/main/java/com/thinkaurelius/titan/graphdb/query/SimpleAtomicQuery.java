package com.thinkaurelius.titan.graphdb.query;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.relations.AttributeUtil;
import com.thinkaurelius.titan.graphdb.relations.EdgeDirection;
import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;
import com.thinkaurelius.titan.graphdb.vertices.InternalTitanVertex;
import com.thinkaurelius.titan.graphdb.vertices.RemovableRelationIterable;
import com.thinkaurelius.titan.graphdb.vertices.RemovableRelationIterator;
import com.thinkaurelius.titan.util.datastructures.IterablesUtil;
import com.thinkaurelius.titan.util.interval.*;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Query;
import com.tinkerpop.blueprints.Vertex;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class SimpleAtomicQuery implements AtomicQuery {

    private static final Map<String,Object> NO_CONSTRAINTS = ImmutableMap.of();

    protected final InternalTitanTransaction tx;
	private InternalTitanVertex node;
    private long nodeid;
    
    private boolean inMemoryRetrieval=false;
    
    private Direction dir;
    protected TitanType type;
    private TypeGroup group;
    private Map<String,Object> constraints;

    private boolean queryProps;
    private boolean queryRelships;
    private boolean queryHidden;
    private boolean queryUnmodifiable;

    private long limit = Long.MAX_VALUE;
    

    public SimpleAtomicQuery(InternalTitanTransaction tx) {
        Preconditions.checkNotNull(tx);
        this.tx=tx;
        if (tx!=null && tx.isClosed()) throw GraphDatabaseException.transactionNotOpenException();

        dir = null;
        type = null;
        group = null;
        queryProps=true;
        queryRelships=true;
        queryHidden=false;
        queryUnmodifiable=true;
        constraints = NO_CONSTRAINTS;
    }
    
    public SimpleAtomicQuery(InternalTitanVertex n) {
        this(n.getTransaction());


        node = n;
        if (node.hasID()) nodeid=node.getID();
        else nodeid = -1;   
    }
    
    public SimpleAtomicQuery(InternalTitanTransaction tx, long nodeid) {
        this(tx);

        Preconditions.checkArgument(nodeid>0);
        //if (!tx.containsVertex(nodeid)) throw new InvalidNodeException("TitanVertex with given ID does not exist: " + nodeid);
        node = null;
        this.nodeid = nodeid;
    }

    SimpleAtomicQuery(SimpleAtomicQuery q) {
        dir = q.dir;
        type= q.type;
        group = q.group;
        queryProps=q.queryProps;
        queryRelships=q.queryRelships;
        queryHidden=q.queryHidden;
        queryUnmodifiable=q.queryUnmodifiable;
        
        node = q.node;
        nodeid = q.nodeid;
        tx = q.tx;
        
        inMemoryRetrieval=q.inMemoryRetrieval;
        limit = q.limit;
        if (q.constraints==NO_CONSTRAINTS) constraints = NO_CONSTRAINTS;
        else constraints = new HashMap<String,Object>(q.constraints);
    }

	SimpleAtomicQuery(InternalTitanVertex node, SimpleAtomicQuery q) {
		this(q);
		this.node=node;
	}
	
	@Override
	public SimpleAtomicQuery clone() {
		SimpleAtomicQuery q = new SimpleAtomicQuery(this);
		return q;
	}

	public static final SimpleAtomicQuery queryAll(InternalTitanVertex node) {
		return new SimpleAtomicQuery(node).includeHidden();
	}
    
    protected final TitanType getType(String typeName) {
        TitanType t = tx.getType(typeName);
        if (t==null && !tx.getTxConfiguration().getAutoEdgeTypeMaker().ignoreUndefinedQueryTypes()) {
            throw new IllegalArgumentException("Undefined type used in query: " + typeName);
        }
        return t;
    }
    
    private final TitanKey getKey(String keyName) {
        TitanType t = getType(keyName);
        if (t instanceof TitanKey) return (TitanKey)t;
        else throw new IllegalArgumentException("Provided name does not represent a key: " + keyName);
    }

	/* ---------------------------------------------------------------
	 * Query Construction
	 * ---------------------------------------------------------------
	 */

    public void removeConstraint(TitanType ptype) {
        removeConstraint(ptype.getName());
    }

    public void removeConstraint(String ptype) {
        if (constraints.containsKey(ptype))
            constraints.remove(ptype);
    }

    private<T> AtomicQuery withPropertyConstraint(TitanType type,Object value) {
        Preconditions.checkNotNull(type);
        if (constraints == NO_CONSTRAINTS) constraints = new HashMap<String,Object>(5);
        String typeName = type.getName();
        if (constraints.containsKey(typeName)) {
            if (!constraints.get(typeName).equals(value))
                throw new IllegalArgumentException("Conflicting constraints set for property: " + type);
        } else constraints.put(type.getName(), value);
        return this;
    }

	
    @Override
    public AtomicQuery has(TitanType type, Object value) {
        if (type.isEdgeLabel()) {
            Preconditions.checkArgument(((TitanLabel)type).isUnidirected(),"Only unidirectional edges supported in query constraint.");
            if (value!=null) {
                Preconditions.checkArgument(value instanceof TitanVertex,"Value needs to be a vertex.");
            }
            return withPropertyConstraint(type,value);
        } else {
            assert type.isPropertyKey();
            if (value!=null) {
                value = AttributeUtil.verifyAttribute((TitanKey)type,value);
                return withPropertyConstraint(type,new PointInterval(value));
            } else {
                return withPropertyConstraint(type,DoesNotExist.INSTANCE);
            }
        }
    }


    @Override
    public AtomicQuery has(String type, Object value) {
        TitanType t = getType(type);
        if (t==null) {
            if (value==null) return this;
            else return EmptyAtomicQuery.INSTANCE;
        } else return has(t, value);
    }

    @Override
    public<T extends Comparable<T>> AtomicQuery interval(TitanKey key, T start, T end) {
        Preconditions.checkNotNull(start);
        Preconditions.checkNotNull(end);
        Preconditions.checkNotNull(key);
        Object s = start, e = end;  //stupid javac workaround
        if (key.getDataType().equals(Long.class) && ((s instanceof Integer) || (e instanceof Integer)) ) {
            //Automatic cast in case of correctable type mismatch
            return interval(key,Long.valueOf(((Number)s).longValue()),Long.valueOf(((Number)e).longValue()));
        }
        AttributeUtil.checkAttributeType(key,start);
        AttributeUtil.checkAttributeType(key,end);
        return withPropertyConstraint(key, new Range(start, end));
    }

    @Override
    public<T extends Comparable<T>> AtomicQuery interval(String key, T start, T end) {
        TitanKey k = getKey(key);
        if (k==null) return EmptyAtomicQuery.INSTANCE;
        else return interval(k, start, end);
    }
    
    @Override
    public<T extends Comparable<T>> AtomicQuery has(String key, T value, Query.Compare compare) {
        TitanKey k = getKey(key);
        if (k==null) return EmptyAtomicQuery.INSTANCE;
        else return has(k, value, compare);
    }

    public<T extends Comparable<T>> AtomicQuery has(TitanKey key, T value, Query.Compare compare) {
        Preconditions.checkNotNull(compare);
        Preconditions.checkNotNull(key);
        if (compare==Compare.EQUAL) return has(key,value);
        if (value!=null) {
            Object v = value; //stupid javac workaround
            if (key.getDataType().equals(Long.class) && v instanceof Integer) {
                //Automatic cast in case of correctable type mismatch
                return has(key,Long.valueOf(((Number)v).longValue()),compare);
            }
            AttributeUtil.checkAttributeType(key,value);
        }
        AtomicInterval interval = IntervalUtil.getInterval(value,compare);

        if (constraints == NO_CONSTRAINTS) constraints = new HashMap<String,Object>(5);
        if (constraints.containsKey(key.getName())) {
            Object o = constraints.get(key.getName());
            assert (o instanceof AtomicInterval);
            interval = ((AtomicInterval)o).intersect(interval);
            if (interval==null) return EmptyAtomicQuery.INSTANCE;
//          Preconditions.checkNotNull(interval,"Additional constraint leads to empty intersection and is therefore infeasible!");
        }
        constraints.put(key.getName(), interval);
        return this;
    }

    @Override
    public TitanQuery types(TitanType... type) {
        throw new UnsupportedOperationException("Not supported in atomic query.");
    }

    @Override
    public TitanQuery labels(String... labels) {
        throw new UnsupportedOperationException("Not supported in atomic query.");
    }

    @Override
    public TitanQuery keys(String... keys) {
        throw new UnsupportedOperationException("Not supported in atomic query.");
    }

    @Override
    public SimpleAtomicQuery type(TitanType type) {
        Preconditions.checkNotNull(type);
        this.type=type;
        if (type.isEdgeLabel()) {
            edgesOnly();
            if (dir==null) {
                if (((TitanLabel)type).isUnidirected()) dir = Direction.OUT;
                else dir = Direction.BOTH;
            }
        }
        else {
            assert type.isPropertyKey();
            propertiesOnly();
        }
        return this;
    }
    
    @Override
    public AtomicQuery type(String type) {
        TitanType t = getType(type);
        if (t==null) return EmptyAtomicQuery.INSTANCE;
        else return type(t);
    }

    @Override
	public SimpleAtomicQuery group(TypeGroup group) {
        Preconditions.checkNotNull(group);
        this.group=group;
        this.type=null;
        allEdges();
		return this;
	}
	

	@Override
	public SimpleAtomicQuery direction(Direction d) {
        dir = d;
		return this;
	}

	@Override
	public SimpleAtomicQuery onlyModifiable() {
        queryUnmodifiable=false;
		return this;
	}

	@Override
	public SimpleAtomicQuery includeHidden() {
        queryHidden=true;
		return this;
	}

    @Override
    public SimpleAtomicQuery limit(long limit) {
        this.limit=limit;
        return this;
    }

    @Override
    public SimpleAtomicQuery inMemory() {
        this.inMemoryRetrieval=true;
        return this;
    }



    void propertiesOnly() {
        dir = Direction.OUT;
        queryProps=true;
        queryRelships=false;
    }

    void edgesOnly() {
        queryProps=false;
        queryRelships=true;
    }

    void allEdges() {
        queryProps=true;
        queryRelships=true;
    }


    /* ---------------------------------------------------------------
      * Query Inspection
      * ---------------------------------------------------------------
      */

    @Override
    public boolean hasEdgeTypeCondition() {
        return type!=null;
    }

    @Override
    public TitanType getTypeCondition() {
        if (!hasEdgeTypeCondition()) throw new IllegalStateException("This query does not have an edge type condition");
        return type;
    }


    @Override
    public TypeGroup getGroupCondition() {
        if (!hasGroupCondition()) throw new IllegalStateException("This query does not have a group condition!");
        return group;
    }

    @Override
    public boolean hasGroupCondition() {
        return group!=null;
    }

    @Override
    public boolean hasDirectionCondition() {
        return dir!=null;
    }

    @Override
    public Direction getDirectionCondition() {
        if (!hasDirectionCondition()) throw new IllegalStateException("This query does not have a direction condition");
        return dir;
    }

    @Override
    public boolean isAllowedDirection(EdgeDirection _dir) {
        if (dir==null) return true;
        else return _dir.impliedBy(dir);
    }

    @Override
    public boolean queryProperties() {
        return queryProps;
    }

    @Override
    public boolean queryRelationships() {
        return queryRelships;
    }


    @Override
    public boolean queryHidden() {
        return queryHidden;
    }

    @Override
    public boolean queryUnmodifiable() {
        return queryUnmodifiable;
    }

    @Override
    public long getVertexID() {
        if (nodeid>0) return nodeid;
        else throw new IllegalStateException("Vertex id has not been set for query");
    }

    @Override
    public InternalTitanVertex getNode() {
        return node;
    }

    @Override
    public long getLimit() {
        return limit;
    }

    @Override
    public boolean hasConstraints() {
        return !constraints.isEmpty();
    }

    @Override
    public Map<String,Object> getConstraints() {
        return constraints;
    }


	/* ---------------------------------------------------------------
	 * Query Execution
	 * ---------------------------------------------------------------
	 */
	
	@Override
	public Iterable<TitanProperty> properties() {
		propertiesOnly();
		return new RemovableRelationIterable<TitanProperty>(node.getRelations(this, true));
	}

    @Override
	public Iterator<TitanProperty> propertyIterator() {
		propertiesOnly();
		return new RemovableRelationIterator<TitanProperty>(node.getRelations(this, true).iterator());
	}

    @Override
	public Iterator<TitanEdge> edgeIterator() {
		edgesOnly();
		return new RemovableRelationIterator<TitanEdge>(node.getRelations(this, true).iterator());
	}

	@Override
	public Iterable<Edge> edges() {
		edgesOnly();
		return (Iterable)new RemovableRelationIterable<TitanEdge>(node.getRelations(this, true));
	}

    @Override
    public Iterable<TitanEdge> titanEdges() {
        edgesOnly();
        return new RemovableRelationIterable<TitanEdge>(node.getRelations(this, true));
    }

    @Override
    public Iterator<TitanRelation> relationIterator() {
		allEdges();
		return new RemovableRelationIterator<TitanRelation>(node.getRelations(this, true).iterator());
	}

	@Override
	public Iterable<TitanRelation> relations() {
		allEdges();
		return new RemovableRelationIterable<TitanRelation>(node.getRelations(this, true));
	}
	
    @Override
    public long count() {
        edgesOnly();
        return Iterators.size(edgeIterator());
    }

    @Override
    public long propertyCount() {
        propertiesOnly();
        return Iterators.size(propertyIterator());
    }


    //############## Neighborhood ############

    private boolean retrieveInMemory() {
        if (inMemoryRetrieval) return true;
        else if (!QueryUtil.queryCoveredByDiskIndexes(this)) return true;
        else {
            if (node!=null) {
                if (node.isLoaded() && node.hasID()) return false;
                else return true;
            } else {
                if (tx.hasModifications()) return true;
                else return false;
            }
        }
    }

    @Override
    public VertexListInternal vertexIds() {
        Preconditions.checkNotNull(tx);
        if (retrieveInMemory()) {
            return retrieveFromMemory(new VertexArrayList());
        } else {
            return getVertexIDs();
        }
    }

    @Override
    public Iterable<Vertex> vertices() {
        return (Iterable)vertexIds();
    }

    private VertexListInternal retrieveFromMemory(VertexListInternal vertices) {
        edgesOnly();
        if (node==null) node = tx.getExistingVertex(nodeid);
        SimpleAtomicQuery q = new SimpleAtomicQuery(node,this);
        Iterator<TitanEdge> iter = q.edgeIterator();
        while (iter.hasNext()) {
            TitanEdge next = iter.next();
            vertices.add(next.getOtherVertex(node));
        }
        return vertices;
    }

    public VertexListInternal getVertexIDs() {
        edgesOnly();
        Preconditions.checkNotNull(tx);
        Preconditions.checkArgument(node==null || (!node.isNew() && !node.isModified()),
                "Cannot query for raw neighborhood on new or modified node.");
        if (!retrieveInMemory()) {
            Preconditions.checkArgument(nodeid>0,"The node id could not be determined!");
            return new VertexLongList(tx,tx.getRawNeighborhood(this));
        } else {
            return retrieveFromMemory(new VertexLongList(tx));
        }
    }
	
	
	
}
