package com.thinkaurelius.titan.graphdb.transaction;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.exceptions.InvalidEntityException;
import com.thinkaurelius.titan.exceptions.InvalidNodeException;
import com.thinkaurelius.titan.graphdb.database.GraphDB;
import com.thinkaurelius.titan.graphdb.edgequery.InternalEdgeQuery;
import com.thinkaurelius.titan.graphdb.edgequery.StandardEdgeQuery;
import com.thinkaurelius.titan.graphdb.edges.InternalEdge;
import com.thinkaurelius.titan.graphdb.edges.factory.EdgeFactory;
import com.thinkaurelius.titan.graphdb.edgetypes.InternalEdgeType;
import com.thinkaurelius.titan.graphdb.edgetypes.manager.EdgeTypeManager;
import com.thinkaurelius.titan.graphdb.edgetypes.system.SystemPropertyType;
import com.thinkaurelius.titan.graphdb.idmanagement.IDInspector;
import com.thinkaurelius.titan.graphdb.vertices.InternalNode;
import com.thinkaurelius.titan.graphdb.vertices.StandardReferenceNode;
import com.thinkaurelius.titan.graphdb.vertices.factory.NodeFactory;
import com.thinkaurelius.titan.traversal.AllRelationshipsIterable;
import com.thinkaurelius.titan.util.datastructures.Factory;
import com.thinkaurelius.titan.util.datastructures.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractGraphTx implements GraphTx {

	private static final Logger log = LoggerFactory.getLogger(AbstractGraphTx.class);

    protected GraphDB graphdb;

	protected final EdgeTypeManager etManager;
	protected final NodeFactory nodeFactory;
	protected final EdgeFactory edgeFactory;
	
	private ConcurrentMap<PropertyType,ConcurrentMap<Object,Node>> keyIndex;
    private final Lock keyedPropertyCreateLock;
    private ConcurrentMap<PropertyType,Multimap<Object,Node>> attributeIndex;


    private Set<InternalNode> newNodes;
    private NodeCache nodeCache;

    private boolean isOpen;
	private final GraphTransactionConfig config;

	public AbstractGraphTx(GraphDB g, NodeFactory nodeFac, EdgeFactory edgeFac,
			EdgeTypeManager etManage, GraphTransactionConfig config) {
		graphdb = g;
        etManager = etManage;
		nodeFactory = nodeFac;
		edgeFactory = edgeFac;
		edgeFactory.setTransaction(this);
		

		this.config=config;
		isOpen = true;

		if (!config.isReadOnly()) { //TODO: don't maintain newNodes for batch loading transactions
            newNodes = Collections.newSetFromMap(new ConcurrentHashMap<InternalNode,Boolean>(10,0.75f,2));
        } else {
            newNodes=null;
        }
        nodeCache = new StandardNodeCache();

		keyIndex = new ConcurrentHashMap<PropertyType,ConcurrentMap<Object,Node>>(20,0.75f,2);
        attributeIndex = new ConcurrentHashMap<PropertyType,Multimap<Object,Node>>(20,0.75f,2);
        keyedPropertyCreateLock = new ReentrantLock();
	}


	protected void verifyWriteAccess() {
		if (config.isReadOnly()) throw new UnsupportedOperationException("Cannot create new entities in read-only transaction!");
	}
	
	/* ---------------------------------------------------------------
	 * Node and Edge creation
	 * ---------------------------------------------------------------
	 */
	
    @Override
    public void registerNewEntity(InternalNode n) {
        assert (!(n instanceof InternalEdge) || !((InternalEdge)n).isInline());
        assert n.isNew();
        assert !n.hasID();
        
        boolean isNode = !(n instanceof InternalEdge);
        if (config.assignIDsImmediately()) {
            graphdb.assignID(n);           
            if (isNode) nodeCache.add(n,n.getID());
        } else if (newNodes!=null) {
            if (isNode) {
                newNodes.add(n);
            }
        }
    }


	@Override
	public Node createNode() {
		verifyWriteAccess();
		InternalNode n = nodeFactory.createNew(this);
		return n;
	}
    

    @Override
    public boolean containsNode(long id) {
        if (nodeCache.contains(id)) return true;
        else return false;
    }

    @Override
    public Node getNode(long id) {
        if (getTxConfiguration().doVerifyNodeExistence() &&
                !containsNode(id)) throw new InvalidNodeException("Node does not exist!");
        return getExistingNode(id);
    }


    @Override
    public InternalNode getExistingNode(long id) {
        return getExisting(id);
    }

    private InternalNode getExisting(long id) {
        synchronized(nodeCache) {
            InternalNode node = nodeCache.get(id);
            if (node==null) {
                IDInspector idspec = graphdb.getIDInspector();

                if (idspec.isEdgeTypeID(id)) {
                    node = etManager.getEdgeType(id, this);
                } else if (graphdb.isReferenceNodeID(id)) {
                    return new StandardReferenceNode(this, id);
                } else if (idspec.isNodeID(id)) {
                    node = nodeFactory.createExisting(this,id);
                } else throw new IllegalArgumentException("ID could not be recognized!");
                nodeCache.add(node, id);
            }
            return node;
        }

    }
    
    @Override
    public void deleteNode(InternalNode n) {
        boolean removed;
        if (n.hasID()) {
            removed = nodeCache.remove(n.getID());
        } else {
            if (newNodes!=null) removed = newNodes.remove(n);
            else removed=true;
        }
        assert removed;
    }


	@Override
	public Property createProperty(PropertyType relType, Node node,	Object attribute) {
		//Check that attribute of keyed propertyType is unique
		if (relType.isKeyed()) {
			keyedPropertyCreateLock.lock();
			if (config.doVerifyKeyUniqueness() && getNodeByKey(relType, attribute)!=null)
				throw new InvalidEntityException("The specified attribute is already used as a key for the given property type: " + attribute);
		}
			
		InternalEdge e = edgeFactory.createNewProperty(relType, (InternalNode)node, attribute);
		addedEdge(e);
		if (relType.isKeyed()) keyedPropertyCreateLock.unlock();
		return (Property)e;
	}


	@Override
	public Property createProperty(String relType, Node node, Object attribute) {
		return createProperty(getPropertyType(relType),node,attribute);
	}


	@Override
	public Relationship createRelationship(RelationshipType relType, Node start, Node end) {
		InternalEdge e = edgeFactory.createNewRelationship(relType, (InternalNode)start, (InternalNode)end);
		addedEdge(e);
		return (Relationship)e;
	}


	@Override
	public Relationship createRelationship(String relType, Node start, Node end) {
		return createRelationship(getRelationshipType(relType),start,end);
	}
	

	@Override
	public EdgeTypeMaker createEdgeType() {
		verifyWriteAccess();
		return etManager.getEdgeTypeMaker(this);
	}

	@Override
	public boolean containsEdgeType(String name) {
		Map<Object,Node> subindex = keyIndex.get(SystemPropertyType.EdgeTypeName);
		if (subindex==null || !subindex.containsKey(name)) {
            return etManager.containsEdgeType(name, this);
        } else return true;
	}

    @Override
    public EdgeType getEdgeType(String name) {
        Map<Object,Node> subindex = keyIndex.get(SystemPropertyType.EdgeTypeName);
        EdgeType et = null;
        if (subindex!=null) {
            et = (EdgeType)subindex.get(name);
        }
        if (et==null) {
            //Second, check EdgeTypeManager
            InternalEdgeType eti = etManager.getEdgeType(name, this);
            if (eti!=null)
                nodeCache.add(eti, eti.getID());
            et=eti;
        }
        return et;
    }

	@Override
	public PropertyType getPropertyType(String name) {
		EdgeType et = getEdgeType(name);
		if (et==null) {
            if (config.doAutoCreateEdgeTypes()) return config.getAutoEdgeTypeMaker().makePropertyType(name,createEdgeType());
			else throw new IllegalArgumentException("PropertyType with given name does not exist: " + name);
        } else if (et.isPropertyType()) {
			return (PropertyType)et;
		} else throw new IllegalArgumentException("The EdgeType of given name is not a PropertyType!");

	}
	

	@Override
	public RelationshipType getRelationshipType(String name) {
		EdgeType et = getEdgeType(name);
		if (et==null) {
            if (config.doAutoCreateEdgeTypes()) return config.getAutoEdgeTypeMaker().makeRelationshipType(name,createEdgeType());
            throw new IllegalArgumentException("RelationType with given name does not exist: " + name);
        } else if (et.isRelationshipType()) {
			return (RelationshipType)et;
		} else throw new IllegalArgumentException("The EdgeType of given name is not a RelationshipType! " + name);
	}
	

	@Override
	public void addedEdge(InternalEdge edge) {
		verifyWriteAccess();
		Preconditions.checkArgument(edge.isNew());
	}
	
	@Override
	public void deletedEdge(InternalEdge edge) {
		verifyWriteAccess();
		if (edge.isProperty() && !edge.isInline()) {
			Property prop = (Property)edge;
			if (prop.getPropertyType().hasIndex()) {
				removeKeyFromIndex(prop);
			}
		}
	}
	
	
	@Override
	public void loadedEdge(InternalEdge edge) {
		if (edge.isProperty() && !edge.isInline()) {
			Property prop = (Property)edge;
            if (prop.getPropertyType().hasIndex()) {
			    addProperty2Index(prop);
            }
		}
	}
	
	@Override
	public InternalEdgeQuery makeEdgeQuery(InternalNode n) {
		return new StandardEdgeQuery(n);
	}

	@Override
	public EdgeQuery makeEdgeQuery(long nodeid) {
		return new StandardEdgeQuery((InternalNode)getNode(nodeid));
	}

	@Override
	public Iterable<? extends Node> getAllNodes() {
        Iterable<InternalNode> iter = null;
        if (newNodes!=null) 
            iter = Iterables.concat(nodeCache.getAll(),newNodes);
        else iter = nodeCache.getAll();
        
        iter = Iterables.filter(iter,new Predicate<InternalNode>() {
            @Override
            public boolean apply( InternalNode input) {
                assert !(input instanceof Edge);
                if (input instanceof EdgeType) return false;
                else return true;
            }
        });
        return iter;
	}


	@Override
	public Iterable<Relationship> getAllRelationships() {
		return AllRelationshipsIterable.of(getAllNodes());
	}

	
	/* ---------------------------------------------------------------
	 * Index Handling
	 * ---------------------------------------------------------------
	 */	
	
	private void addProperty2Index(Property property) {
	    addProperty2Index(property.getPropertyType(), property.getAttribute(), property.getStart());
	}

	private static Factory<ConcurrentMap<Object,Node>> keyIndexFactory = new Factory<ConcurrentMap<Object,Node>>() {
		@Override
		public ConcurrentMap<Object, Node> create() {
			return new ConcurrentHashMap<Object,Node>(10,0.75f,4);
		}
	};

    private static Factory<Multimap<Object,Node>> attributeIndexFactory = new Factory<Multimap<Object,Node>>() {
        @Override
        public Multimap<Object, Node> create() {
            Multimap<Object,Node> map = ArrayListMultimap.create(10,20);
            return map;
            //return Multimaps.synchronizedSetMultimap(map);
        }
    };
	
	protected void addProperty2Index(PropertyType type, Object att, Node node) {
        Preconditions.checkArgument(type.hasIndex());
		if (type.isKeyed()) {
            //TODO ignore NO-ENTRTY
            ConcurrentMap<Object,Node> subindex = Maps.putIfAbsent(keyIndex, type, keyIndexFactory);

            Node oth = subindex.putIfAbsent(att, node);
            if (oth!=null && !oth.equals(node)) {
                throw new IllegalArgumentException("The key is already used by another node!");
            }
        } else {
            Multimap<Object,Node> subindex = Maps.putIfAbsent(attributeIndex, type, attributeIndexFactory);
            subindex.put(att,node);
        }
	}
	
	private void removeKeyFromIndex(Property property) {
        Preconditions.checkArgument(property.getPropertyType().hasIndex());
        
		PropertyType type = property.getPropertyType();
		if (type.isKeyed()) {
            Map<Object,Node> subindex = keyIndex.get(type);
            Preconditions.checkNotNull(subindex);
            Node n = subindex.remove(property.getAttribute());
            Preconditions.checkArgument(n!=null && n.equals(property.getStart()));
            //TODO Set to NO-ENTRY node object
        } else {
            boolean hasIdenticalProperty = false;
            for (Property p2 : property.getStart().getProperties(type)) {
                if (!p2.equals(property) && p2.getAttribute().equals(property.getAttribute())) {
                    hasIdenticalProperty=true;
                    break;
                }
            }
            if (!hasIdenticalProperty) {
                Multimap<Object,Node> subindex = attributeIndex.get(type);
                Preconditions.checkNotNull(subindex);
                boolean removed = subindex.remove(property.getAttribute(),property.getStart());
                assert removed;
            }
        }

	}

    // #### Keyed Properties #####

	@Override
	public Node getNodeByKey(PropertyType type, Object key) {
		Preconditions.checkArgument(type.isKeyed());
		Map<Object,Node> subindex = keyIndex.get(type);
		if (subindex==null) {
			return null;
		} else {
            //TODO: check for NO-ENTRY and return null
			return subindex.get(key);
		}
	}
	
	@Override
	public Node getNodeByKey(String type, Object key) {
        if (!containsEdgeType(type)) return null;
		return getNodeByKey(getPropertyType(type),key);
	}

    // #### General Indexed Properties #####

	@Override
	public Set<Node> getNodesByAttribute(String type, Object attribute) {
        if (!containsEdgeType(type)) return ImmutableSet.of();
		else return getNodesByAttribute(getPropertyType(type), attribute);
	}
	

	
	@Override
	public Set<Node> getNodesByAttribute(PropertyType type, Object attribute) {
        Preconditions.checkArgument(type.hasIndex());
        //First, get stuff from disk
        long[] nodeids = getNodeIDsByAttributeFromDisk(type, attribute);
        Set<Node> nodes = new HashSet<Node>(nodeids.length);
        for (int i=0;i<nodeids.length;i++)
            nodes.add(getExistingNode(nodeids[i]));
        //Next, the in-memory stuff
        Multimap<Object,Node> subindex = attributeIndex.get(type);
        if (subindex!=null) {
            nodes.addAll(subindex.get(attribute));
        }
		return nodes;
	}

	
	/* ---------------------------------------------------------------
	 * Transaction Handling
	 * ---------------------------------------------------------------
	 */	
	
	private void close() {
        nodeCache.close(); nodeCache=null;
		keyIndex.clear(); keyIndex=null;
		isOpen=false;
	}

    @Override
    public synchronized void rollingCommit() {
        if (!config.assignIDsImmediately()) throw new UnsupportedOperationException("Rolling commits are only supported for immediate ID assignment.");
    }
	
	
	@Override
	public synchronized void commit() {
		close();
	}

	@Override
	public synchronized void abort() {
		close();
	}
	
	
	@Override
	public EdgeFactory getEdgeFactory() {
		return edgeFactory;
	}
	
	@Override
	public boolean isOpen() {
		return isOpen;
	}
	
	@Override
	public boolean isClosed() {
		return !isOpen;
	}
	
	@Override
	public GraphTransactionConfig getTxConfiguration() {
		return config;
	}

	@Override
	public boolean hasModifications() {
		return !config.isReadOnly() && newNodes!=null && !newNodes.isEmpty();
	}















	
}
