package com.thinkaurelius.titan.traversal;

import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.edgetypes.DirWrapper;

import java.util.*;

/**
 * Provides an abstract implementation of {@link Traversal} which proceeds breadth-first.
 * 
 * This generic implementation of {@link Traversal} traverses the graph according to user preferences in a breadth-first manner.
 * It provides an iterator which visits nodes in the underlying graph as they are encountered during the traversal.
 * 
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 *
 * @param <V> Type of subclass extending this class
 */
public class AbstractTraversal<V> implements Traversal<V> {

	private static final int defaultDepth = Integer.MAX_VALUE;
	
	private V parent;
	private int depth;
	private List<DirWrapper<RelationshipType>> types;
	private List<DirWrapper<EdgeTypeGroup>> groups;
	
	protected AbstractTraversal() {}
	
	/**
	 * Registers the extending class.
	 * This method needs to be called during initialization by the extending class.
	 * 
	 * @param parent Instance of extending class
	 */
	protected void setParent(V parent) {
		this.parent=parent;
		types = new ArrayList<DirWrapper<RelationshipType>>();
		groups = new ArrayList<DirWrapper<EdgeTypeGroup>>();
		depth = defaultDepth;
	}
	
	
	private void checkDefinition() {
		for (int i=0;i<types.size();i++) {
			DirWrapper<RelationshipType> t = types.get(i);
			for (int j=i+1;j<types.size();j++) {
				DirWrapper<RelationshipType> t2 = types.get(j);
				if (t.get().equals(t2.get()) && t.hasOverlappingDirection(t2))
					throw new IllegalArgumentException("Two overlapping EdgeTypes have been registered with this traversal!");
			}
			for (int j=0;j<groups.size();j++) {
				DirWrapper<EdgeTypeGroup> t2 = groups.get(j);
				if (t.get().getGroup().getID()==t2.get().getID() && t.hasOverlappingDirection(t2))
					throw new IllegalArgumentException("RelationshipType with overlapping EdgeTypeGroup has been registered with this traversal!");
			}
		}
		for (int i=0;i<groups.size();i++) {
			DirWrapper<EdgeTypeGroup> t = groups.get(i);
			for (int j=i+1;j<groups.size();j++) {
				DirWrapper<EdgeTypeGroup> t2 = groups.get(j);
				if (t.get().getID()==t2.get().getID() && t.hasOverlappingDirection(t2))
					throw new IllegalArgumentException("Two overlapping EdgeTypeGroups have been registered with this traversal!");
			}
		}
	}
	
	@Override
	public V addRelationshipType(RelationshipType type, Direction dir) {
		types.add(DirWrapper.wrap(dir,type));
		return parent;
	}

	@Override
	public V addRelationshipType(RelationshipType type) {
		addRelationshipType(type,null);
		return parent;
	}

	@Override
	public V addEdgeTypeGroup(EdgeTypeGroup group, Direction dir) {
		groups.add(DirWrapper.wrap(dir, group));
		return parent;
	}

	@Override
	public V addEdgeTypeGroup(EdgeTypeGroup group) {
		addEdgeTypeGroup(group,null);
		return parent;
	}

	@Override
	public V setDepth(int depth) {
		this.depth=depth;
		return parent;
	}
	
	/**
	 * Runs the traversal from the given set of seed nodes, calling the TraversalEvaluator as the traversal proceeds.
	 * 
	 * @param seed Set of seed nodes to start the traversal from.
	 * @param eval TraversalEvaluator which is called during the traversal
	 * @return The set of all nodes visited during the traversal
	 */
	protected Set<Node> traversal(Set<Node> seed, TraversalEvaluator eval) {
		checkDefinition();
		NodeTraversalIterator iter = new NodeTraversalIterator(seed,eval);
		return iter.traverseAll();
	}
	
	/**
	 * Constructs a traversal iterator starting from the given set of seed nodes, calling the TraversalEvaluator as the traversal proceeds
	 * according to user interaction.
	 * 
	 * @param seed Set of seed nodes to start the traversal from.
	 * @param eval TraversalEvaluator which is called during the traversal
	 * @return NodeTraveralIterator to control the traversal
	 */
	protected NodeTraversalIterator traverse(Set<Node> seed, TraversalEvaluator eval) {
		checkDefinition();
		NodeTraversalIterator iter = new NodeTraversalIterator(seed,eval);
		return iter;
	}
	
	/**
	 * Iterator which visits nodes as they are encountered by a user specified traversal defined by the enclosing class.
	 * 
	 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
	 * 
	 *
	 */
	public class NodeTraversalIterator implements Iterator<Node> {
		
		private final Set<Node> visited;
		private final TraversalEvaluator eval;
		
		private Set<Node> current;
		private Iterator<Node> currentIter;
		private Set<Node> next;
		private int d;
		
		private Node nextNode;
		
		private NodeTraversalIterator(Set<Node> seed, TraversalEvaluator eval) {
			current = seed;
			visited = new HashSet<Node>();
			this.eval = eval;

			d=0;
			next = new HashSet<Node>();
			currentIter = current.iterator();
			nextNode = nextNode();
		}
		
		/**
		 * Returns all nodes that have been visited so far during this traversal.
		 * 
		 * @return Set of all nodes visited so far
		 */
		public Set<Node> getAllVisitedNodes() {
			return visited;
		}
		
		/**
		 * Executes the entire traversal until termination and returns the set of all visited nodes.
		 * @return The set of all visited nodes when traversal is run to completion.
		 */
		public Set<Node> traverseAll() {
			while(hasNext()) next();
			return getAllVisitedNodes();
		}

		@Override
		public boolean hasNext() {
			return nextNode!=null;
		}

		@Override
		public Node next() {
			if (nextNode==null) throw new NoSuchElementException("No additional node in traversal!");
			Node tmp = nextNode;
			nextNode = nextNode();
			return tmp;
		}
		
		private Node nextNode() {
			Node node = null;
			while (node == null) {
				if (!currentIter.hasNext()) {
					current = next;
					currentIter = current.iterator();
					next = new HashSet<Node>();
					d++;
				}
				if (currentIter.hasNext()) {
					node = currentIter.next();
					if (!eval.nextNode(node)) node = null;
				} else break;
			}

			if (node!=null) {
				//Add neighborhood and add this node to the visited set
				if (d<depth) {
					for (DirWrapper<EdgeTypeGroup> groupdir : groups) {
						Iterable<Relationship> rels = null;
						if (groupdir.hasDirection()) {
							rels = node.getRelationships(groupdir.get(), groupdir.getDirection());
						} else {
							rels = node.getRelationships(groupdir.get());
						}
						exploreNeighborhood(node, rels,visited,current,next,eval);
					}
					for (DirWrapper<RelationshipType> typedir : types) {
						Iterable<Relationship> rels = null;
						if (typedir.hasDirection()) {
							rels = node.getRelationships(typedir.get(), typedir.getDirection());
						} else {
							rels = node.getRelationships(typedir.get());
						}
						exploreNeighborhood(node, rels,visited,current,next,eval);
					}
					if (types.isEmpty() && groups.isEmpty())
						exploreNeighborhood(node, node.getRelationships(),visited,current,next,eval);
				}
				visited.add(node);
			}
			return node;
		}

		/**
		 * Removing nodes is not supported!
		 * @throws UnsupportedOperationException if invoked
		 */
		@Override
		public void remove() {
			throw new UnsupportedOperationException("Cannot remove nodes during traversal!");
		}
			
	}

	
	private static final void exploreNeighborhood(Node node, Iterable<Relationship> rels, Set<Node> visited, Set<Node> current, Set<Node> next, TraversalEvaluator eval) {
		for (Relationship rel : rels) {
			for (Node n : rel.getNodes()) {
				if (!current.contains(n) && !visited.contains(n)) {
					next.add(n);
				}
			}
			eval.nextRelationship(rel);
		}
	}

}
