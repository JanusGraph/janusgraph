package com.thinkaurelius.titan.graphdb.edgequery;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.edges.EdgeDirection;
import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;
import com.thinkaurelius.titan.graphdb.vertices.InternalTitanVertex;
import com.thinkaurelius.titan.graphdb.vertices.RemovableEdgeIterable;
import com.thinkaurelius.titan.graphdb.vertices.RemovableEdgeIterator;
import com.thinkaurelius.titan.util.interval.AtomicInterval;
import com.thinkaurelius.titan.util.interval.IntervalUtil;
import com.thinkaurelius.titan.util.interval.PointInterval;
import com.thinkaurelius.titan.util.interval.Range;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Query;
import com.tinkerpop.blueprints.Vertex;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class AtomicTitanQuery implements InternalTitanQuery {

    protected final InternalTitanTransaction tx;
	private InternalTitanVertex node;
    private long nodeid;
    
    private boolean inMemoryRetrieval=false;
    
    private Direction dir;
    private TitanType type;
    private TypeGroup group;
    private Map<String,Object> constraints;

    private boolean queryProps;
    private boolean queryRelships;
    private boolean queryHidden;
    private boolean queryUnmodifiable;

    private long limit = Long.MAX_VALUE;
    

    public AtomicTitanQuery(InternalTitanTransaction tx) {
        this.tx=tx;
        
        dir = null;
        type = null;
        group = null;
        queryProps=true;
        queryRelships=true;
        queryHidden=false;
        queryUnmodifiable=true;
        constraints = null;
    }
    
    public AtomicTitanQuery(InternalTitanVertex n) {
        this(n.getTransaction());
        Preconditions.checkNotNull(n);


        node = n;
        if (node.hasID()) nodeid=node.getID();
        else nodeid = -1;   
    }
    
    public AtomicTitanQuery(InternalTitanTransaction tx, long nodeid) {
        this(tx);

        Preconditions.checkArgument(nodeid>0);
        //if (!tx.containsVertex(nodeid)) throw new InvalidNodeException("TitanVertex with given ID does not exist: " + nodeid);
        node = null;
        this.nodeid = nodeid;
    }

    AtomicTitanQuery(AtomicTitanQuery q) {
        dir = q.dir;
        type = q.type;
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
        if (q.constraints ==null) constraints =null;
        else constraints = new HashMap<String,Object>(q.constraints);
    }

	AtomicTitanQuery(InternalTitanVertex node, AtomicTitanQuery q) {
		this(q);
		this.node=node;
	}
	
	@Override
	public AtomicTitanQuery copy() {
		AtomicTitanQuery q = new AtomicTitanQuery(this);
		return q;
	}

	public static final AtomicTitanQuery queryAll(InternalTitanVertex node) {
		return new AtomicTitanQuery(node).includeHidden();
	}

    public boolean isAtomic() {
        return true;
    }

	/* ---------------------------------------------------------------
	 * Query Construction
	 * ---------------------------------------------------------------
	 */

    public void removeConstraint(TitanType ptype) {
        if (constraints==null) return;
        constraints.remove(ptype.getName());
    }

    public void removeConstraint(String ptype) {
        removeConstraint(tx.getType(ptype));
    }

    private<T> AtomicTitanQuery withPropertyConstraint(TitanType ptype,Object value) {
        Preconditions.checkNotNull(ptype);
        if (constraints == null) constraints = new HashMap<String,Object>(5);
        Preconditions.checkArgument(!constraints.containsKey(ptype.getName()),"Conflicting constraint already exists for property.");
        constraints.put(ptype.getName(), value);
        return this;
    }

	
    @Override
    public AtomicTitanQuery has(TitanType etype, Object value) {
        if (etype.isEdgeLabel()) {
            Preconditions.checkArgument(((TitanLabel)etype).isUnidirected(),"Only unidirectional edges supported inline.");
            Preconditions.checkArgument(value instanceof TitanVertex,"Value needs to be a vertex.");
            return withPropertyConstraint(etype,value);
        } else {
            assert etype.isPropertyKey();
            Preconditions.checkArgument(((TitanKey) etype).getDataType().isInstance(value), "Value is not an INSTANCE of the property type's data type.");
            return withPropertyConstraint(etype,new PointInterval(value));
        }
    }


    @Override
    public AtomicTitanQuery has(String ptype, Object value) {
        if (!tx.containsType(ptype)) throw new IllegalArgumentException("Unknown property type: " + ptype);
        return has(tx.getType(ptype), value);
    }

    @Override
    public<T extends Comparable<T>> TitanQuery interval(TitanKey ptype, T start, T end) {
        Preconditions.checkNotNull(start);
        Preconditions.checkNotNull(end);
        Preconditions.checkNotNull(ptype);
        return withPropertyConstraint(ptype, new Range(start, end));
    }

    @Override
    public<T extends Comparable<T>> TitanQuery interval(String ptype, T start, T end) {
        if (!tx.containsType(ptype)) throw new IllegalArgumentException("Unknown property type: " + ptype);
        return interval(tx.getPropertyKey(ptype), start, end);
    }
    
    @Override
    public<T extends Comparable<T>> TitanQuery has(String ptype, T value, Query.Compare compare) {
        if (!tx.containsType(ptype)) throw new IllegalArgumentException("Unknown property type: " + ptype);
        return has(tx.getPropertyKey(ptype),value,compare);
    }

    public<T extends Comparable<T>> TitanQuery has(TitanKey ptype, T value, Query.Compare compare) {
        Preconditions.checkNotNull(value);
        Preconditions.checkNotNull(compare);
        Preconditions.checkNotNull(ptype);
        AtomicInterval<T> interval = IntervalUtil.getInterval(value,compare);

        if (constraints == null) constraints = new HashMap<String,Object>(5);
        if (constraints.containsKey(ptype.getName())) {
            Object o = constraints.get(ptype.getName());
            assert (o instanceof AtomicInterval);
            interval = ((AtomicInterval<T>)o).intersect(interval);
            Preconditions.checkNotNull(interval,"Additional constraint leads to empty intersection and is therefore infeasible!");
        }
        constraints.put(ptype.getName(), interval);
        return this;
    }

    @Override
    public AtomicTitanQuery labels(String... type) {
        if (type.length==0) {
            this.type=null;
        } else {
            Preconditions.checkArgument(type.length==1);
            type(type[0]);
        }
        return this;
    }

    @Override
    public AtomicTitanQuery keys(String... type) {
        if (type.length==0) {
            this.type=null;
        } else {
            Preconditions.checkArgument(type.length==1);
            type(type[0]);
        }
        return this;
    }
    
    @Override
    public AtomicTitanQuery types(TitanType... type) {
        if (type.length==0) {
            this.type=null;
        } else {
            Preconditions.checkArgument(type.length==1);
            type(type[0]);
        }
        return this;
    }
    
    public AtomicTitanQuery type(String type) {
        Preconditions.checkNotNull(tx);
        return type(tx.getType(type));
    }
    
    public AtomicTitanQuery type(TitanType type) {
        Preconditions.checkNotNull(type);
        this.type = type;
        group = null;
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

    protected void removeEdgeType() {
        type = null;
    }
	
	@Override
	public AtomicTitanQuery group(TypeGroup group) {
        Preconditions.checkNotNull(group);
        this.group=group;
        removeEdgeType();
        allEdges();
		return this;
	}
	

	@Override
	public AtomicTitanQuery direction(Direction d) {
        dir = d;
		return this;
	}

	@Override
	public AtomicTitanQuery onlyModifiable() {
        queryUnmodifiable=false;
		return this;
	}

	@Override
	public AtomicTitanQuery includeHidden() {
        queryHidden=true;
		return this;
	}

    @Override
    public AtomicTitanQuery limit(long limit) {
        this.limit=limit;
        return this;
    }
    
    public AtomicTitanQuery resetRetrievalLimit() {
        this.limit = Long.MAX_VALUE;
        return this;
    }

    @Override
    public AtomicTitanQuery inMemory() {
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
        if (!hasEdgeTypeCondition()) throw new IllegalStateException("This query does not have a edge type condition!");
        return type;
    }


    @Override
    public TypeGroup getEdgeTypeGroupCondition() {
        if (!hasEdgeTypeGroupCondition()) throw new IllegalStateException("This query does not have a edge type group condition!");
        return group;
    }

    @Override
    public boolean hasEdgeTypeGroupCondition() {
        return group!=null;
    }

    @Override
    public boolean hasDirectionCondition() {
        return dir!=null;
    }

    @Override
    public Direction getDirectionCondition() {
        if (!hasDirectionCondition()) throw new IllegalStateException("This query does not have a direction condition!");
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
    public long getNodeID() {
        if (nodeid>0) return nodeid;
        else throw new IllegalStateException("NodeID has not been set for query!");
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
        return constraints !=null && !constraints.isEmpty();
    }

    @Override
    public Map<String,Object> getConstraints() {
        if (constraints ==null) return new HashMap<String, Object>();
        else return constraints;
    }


	/* ---------------------------------------------------------------
	 * Query Execution
	 * ---------------------------------------------------------------
	 */
	
	
	@Override
	public Iterable<TitanProperty> properties() {
		propertiesOnly();
		return new RemovableEdgeIterable<TitanProperty>(node.getRelations(this, true));
	}


	public Iterator<TitanProperty> propertyIterator() {
		propertiesOnly();
		return new RemovableEdgeIterator<TitanProperty>(node.getRelations(this, true).iterator());
	}

	public Iterator<TitanEdge> edgeIterator() {
		edgesOnly();
		return new RemovableEdgeIterator<TitanEdge>(node.getRelations(this, true).iterator());
	}

	@Override
	public Iterable<Edge> edges() {
		edgesOnly();
		return (Iterable)new RemovableEdgeIterable<TitanEdge>(node.getRelations(this, true));
	}

    @Override
    public Iterable<TitanEdge> titanEdges() {
        edgesOnly();
        return new RemovableEdgeIterable<TitanEdge>(node.getRelations(this, true));
    }


    public Iterator<TitanRelation> relationIterator() {
		allEdges();
		return new RemovableEdgeIterator<TitanRelation>(node.getRelations(this, true).iterator());
	}

	@Override
	public Iterable<TitanRelation> relations() {
		allEdges();
		return new RemovableEdgeIterable<TitanRelation>(node.getRelations(this, true));
	}
	
    @Override
    public long count() {
        resetRetrievalLimit();
        edgesOnly();
        return Iterators.size(edgeIterator());
    }

    @Override
    public long propertyCount() {
        resetRetrievalLimit();
        propertiesOnly();
        return Iterators.size(propertyIterator());
    }


    //############## Neighborhood ############

    private boolean retrieveInMemory() {
        if (inMemoryRetrieval) return true;
        else if (!EdgeQueryUtil.queryCoveredByDiskIndexes(this)) return true;
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

    private VertexListInternal retrieveFromMemory(VertexListInternal nodes) {
        if (node==null) node = tx.getExistingVertex(nodeid);
        AtomicTitanQuery q = new AtomicTitanQuery(node,this);
        Iterator<TitanEdge> iter = q.edgeIterator();
        while (iter.hasNext()) {
            TitanEdge next = iter.next();
            nodes.add(next.getOtherVertex(node));
        }
        return nodes;
    }

    public VertexListInternal getVertexIDs() {
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
