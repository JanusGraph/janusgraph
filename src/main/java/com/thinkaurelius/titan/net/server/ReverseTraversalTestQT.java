package com.thinkaurelius.titan.net.server;

import com.thinkaurelius.titan.core.Direction;
import com.thinkaurelius.titan.core.GraphTransaction;
import com.thinkaurelius.titan.core.Node;
import com.thinkaurelius.titan.core.Relationship;
import com.thinkaurelius.titan.core.query.QueryResult;
import com.thinkaurelius.titan.core.query.QueryType;
import com.thinkaurelius.titan.net.server.ReverseTraversalTestQT.TraversalState;
import com.thinkaurelius.titan.net.server.ReverseTraversalTestQT.TraversedEdge;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

public class ReverseTraversalTestQT implements QueryType<TraversalState, TraversedEdge> {

	public static class TraversalState {
		private final long generation;
		private final HashSet<Long> seenNodes;
		public TraversalState(long generation, HashSet<Long> seenNodes) {
			this.generation = generation;
			this.seenNodes = seenNodes;
		}
		
		// For Kryo
		public TraversalState() {
			generation = 0;
			seenNodes = new HashSet<Long>();
		}
	}

	public static class TraversedEdge {
		private long generation;

		private String startGDID;
		private long startID;

		private String endGDID;
		private long endID;

		private String relationshipType;

		public TraversedEdge(long generation, String startGDID, long startID,
				String endGDID, long endID, String relationshipType) {
			this.generation = generation;
			this.startGDID = startGDID;
			this.startID = startID;
			this.endGDID = endGDID;
			this.endID = endID;
			this.relationshipType = relationshipType;
		}

		// For Kyro
		public TraversedEdge() {
			generation = 0;
			startGDID = null;
			endGDID = null;
			startID = 0;
			endID = 0;
			relationshipType = null;
		}

		public String getStartGDID() {
			return startGDID;
		}

		public long getStartID() {
			return startID;
		}

		public String getEndGDID() {
			return endGDID;
		}

		public long getEndID() {
			return endID;
		}

		public String getRelationshipType() {
			return relationshipType;
		}
		
		public String toString() {
			StringBuilder sb = new StringBuilder(64);
			sb.append('[').append(generation).append("] ").
				append('(').append(startID).append(':').append(startGDID).append(") ").
				append("-(").append(relationshipType).append(")-> ").
				append('(').append(endID).append(':').append(endGDID).append(')');
			return sb.toString();
		}
	}
	
	@Override
	public Class<TraversalState> queryType() {
		return TraversalState.class;
	}

	@Override
	public Class<TraversedEdge> resultType() {
		return TraversedEdge.class;
	}

	@Override
	public void answer(GraphTransaction tx, Node anchor, TraversalState query,
			QueryResult<TraversedEdge> result) {
		HashSet<Long> seenNodes = (HashSet<Long>)query.seenNodes;
		long generation = query.generation;
		LinkedList<Node> localTraversalQ = new LinkedList<Node>();
		localTraversalQ.addLast(anchor);
		
		assert !seenNodes.contains(anchor.getID());
		
		List<Node> forwardDestinations = new LinkedList<Node>();
		while (!localTraversalQ.isEmpty()) {
			Node n = localTraversalQ.remove();
			
			if (n.isReferenceNode()) {
				forwardDestinations.add(n);
			} else {
//				for (Edge e : n.getEdges(Direction.In)) {
				for (Relationship e : n.getRelationships(Direction.In)) {

					// Check that node really terminates on "n"
//					Collection<? extends Node> ends = e.getEndNodes();
//					assert null != ends;
//					assert 1 == ends.size();
//					Node end = Iterators.getOnlyElement(ends.iterator());
					Node end = e.getEnd();
					assert end.equals(n);
					
					Node start = e.getStart();
					
					// Send edge
					final String startGDID;
					if (start.isReferenceNode())
						startGDID = "?";
					else
						startGDID = start.getString("ID");
					assert null != startGDID;
					
					final String endGDID = end.getString("ID");
					assert null != endGDID;
					
					String relType = e.getRelationshipType().getName();
					assert null != relType;
					
					result.add(new TraversedEdge(generation, startGDID, start.getID(), endGDID, end.getID(), relType));
					
					// Push start node
					localTraversalQ.addLast(start);
				}
			}
		}
		
		TraversalState nextState = new TraversalState(generation + 1, seenNodes);
		for (Node n : forwardDestinations) {
			n.forwardQuery(nextState);
		}
	}
	

}
