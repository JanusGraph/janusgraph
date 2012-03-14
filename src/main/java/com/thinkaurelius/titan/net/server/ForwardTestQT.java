package com.thinkaurelius.titan.net.server;

import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.core.GraphTransaction;
import com.thinkaurelius.titan.core.Node;
import com.thinkaurelius.titan.core.Property;
import com.thinkaurelius.titan.core.Relationship;
import com.thinkaurelius.titan.core.query.QueryResult;
import com.thinkaurelius.titan.core.query.QueryType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

public class ForwardTestQT implements QueryType<String, String> {
	
	private static final String terminationString =
		"Forwarded=True";
	
	private static final Logger log = 
		LoggerFactory.getLogger(ForwardTestQT.class);

	@Override
	public Class<String> queryType() {
		return String.class;
	}

	@Override
	public Class<String> resultType() {
		return String.class;
	}

	private void sendPropertyAsResult(QueryResult<String> result, Node node, String propName) {
		Property p = Iterators.getOnlyElement(node.getProperties(propName).iterator());
		String value = p.getString();
		log.debug("Sending result: {}={}", propName, value);
		result.add(propName + "=" + value);
	}
	
	@Override
	public void answer(GraphTransaction tx, Node anchor, String query,
			QueryResult<String> result) {
		
		log.info("Starting answering");
		
		if (query.equals(terminationString)) {
			long id = anchor.getID();
			if (anchor.isReferenceNode()) {
				log.error("Anchor node ({}) isReferenceNode() returned true", id);
				return;
			}
			log.info("Node {} is non-reference", id);
			sendPropertyAsResult(result, anchor, "ID");
			log.info("Terminating ({})", terminationString);
			return;
		}
		
		Set<Long> seenNodes = new HashSet<Long>();
		
		LinkedList<Node> nodesToTraverse = new LinkedList<Node>();
		nodesToTraverse.add(anchor);
		int nodesTraversed = 0;
		long selfId = anchor.getID();
		while (! nodesToTraverse.isEmpty()) {
			log.debug("Restarting traversal loop");
			Node n = nodesToTraverse.removeFirst();
			if (n.isReferenceNode()) {
				long remoteNodeID = n.getID();
				log.debug("Forwarding query on node " + remoteNodeID);
				tx.forwardQuery(remoteNodeID, terminationString);
				return;
			}
			log.debug("Fetching edges on node {}", n.getID());
			for (Relationship e : n.getRelationships()) {
				log.debug("Examining endpoints on edge {}", e.getID());
                
//				if (0 == e.getEndNodes().size()) {
//					log.debug("Skipping edge {} due to zero endnodes", e.getID());
//					continue;
//				}
				
				Node end = e.getEnd();
				Node start = e.getStart();
				if (end.getID() == selfId && start.getID() == selfId) {
					log.debug("Skipping selfedge {}", e.getID());
					continue;
				}
				
				if (end.getID() != selfId && start.getID() != selfId) {
					log.debug("Ignoring unexpected edge {} -> {} on node {}", 
							new Object[]{start.getID(), end.getID(), selfId});
					continue;
				}
				
				Node target;
				if (end.getID() == selfId) {
					target = start;
				} else {
					target = end;
				}
				
				nodesTraversed++;
				if (target.isReferenceNode()) {
					long remoteNodeID = target.getID();
					log.debug("Forwarding query on node " + remoteNodeID);
					tx.forwardQuery(remoteNodeID, terminationString);
					return;
				} else {
					long id = target.getID();
					if (seenNodes.contains(id)) {
						log.debug("Skipping node {} (already visited)", id);
					} else {					
						log.debug("Saved local node for future visit: {}", id);
						seenNodes.add(id);
						nodesToTraverse.add(target);
					}
				}
			}
		}
		
		log.debug("Nodes traversed: {}", nodesTraversed);
		
		log.error("Couldn't find a reference vertex starting from " + 
				anchor.getID() + "; stuck in a disconnected subgraph?");
	}
}
