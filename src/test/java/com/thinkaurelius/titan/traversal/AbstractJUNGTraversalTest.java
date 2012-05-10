package com.thinkaurelius.titan.traversal;

import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.core.Node;
import com.thinkaurelius.titan.core.Relationship;
import edu.uci.ics.jung.algorithms.scoring.BetweennessCentrality;
import edu.uci.ics.jung.algorithms.scoring.VertexScorer;
import edu.uci.ics.jung.algorithms.shortestpath.DijkstraShortestPath;
import edu.uci.ics.jung.graph.Graph;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

public abstract class AbstractJUNGTraversalTest extends AbstractSimpleTraversalTest {

	private static final Logger log =
		LoggerFactory.getLogger(AbstractJUNGTraversalTest.class);
	
	protected AbstractJUNGTraversalTest(GraphDatabaseConfiguration config) {
		super(config);
	}

	@Test
	public void jungGraph1() {
		int maxEdges = CHAIN_LENGTH/4*7-1;
		for (int depth = 12;depth<(CHAIN_LENGTH)/2 + 40; depth+=24) {
			log.debug("Depth: {}", depth);
		for (ConversionMode mode : new ConversionMode[]{ConversionMode.Standard,ConversionMode.ForceUndirected}) {
			assertEquals(depth%4,0);
			int edges = Math.min(maxEdges, depth/2*7 - 2);
			int middle = CHAIN_LENGTH/2;
			int other = middle + 4;
			Node seed = tx.getNodeByKey(id, "a"+middle);

			JUNGTraversal jt = new JUNGTraversal(mode);
			jt.setDepth(depth);
			Graph<Node,Relationship> jgraph = jt.construct(seed);
			assertEquals(edges,jgraph.getEdgeCount());
			assertEquals(edges+1,jgraph.getVertexCount());
			DijkstraShortestPath<Node,Relationship> dj = new DijkstraShortestPath<Node,Relationship>(jgraph);
			Node target = tx.getNodeByKey(id, "b"+other);
			assertEquals(5,dj.getDistance(seed, target).intValue());
			
			BetweennessCentrality<Node,Relationship> pr = new BetweennessCentrality<Node,Relationship>(jgraph);
			VertexScorer<Node,Double> vs = pr;
			for (Node n : jgraph.getVertices()) {
				String nids = n.getString(id);
				int nid = Integer.parseInt(nids.substring(1));
				if (nids.startsWith("c")) {
					assertEquals(0.0,vs.getVertexScore(n),0.00001);
				} else if (nids.startsWith("b")) {
					if (nid%4==0) {
						if (mode==ConversionMode.Standard)
							assertEquals(Math.min(CHAIN_LENGTH, 2*depth+1),vs.getVertexScore(n),0.00001);
						else 
							assertEquals(edges-1,vs.getVertexScore(n),0.00001);
					}
					else assertEquals(0.0,vs.getVertexScore(n),0.00001);
				} else { //a
					
				}
				
			}
		}
		}
	}
	
	@Test
	public void jungGraph2() {
		for (int nid = 0;nid<CHAIN_LENGTH; nid++) {
			ConversionMode mode = ConversionMode.IgnoreUndirected;

			Node seed = tx.getNodeByKey(id, "a"+nid);
			//log.debug(nid);
			JUNGTraversal jt = new JUNGTraversal(mode);
			jt.addRelationshipType(a2b);
			jt.addRelationshipType(b2c);
			
			for (int depth = 2;depth<10; depth+=3) {
				jt.setDepth(depth);
				Graph<Node,Relationship> jgraph = jt.construct(seed);
				int edges = nid%2==0?(4-nid%4)/2:0;
				assertEquals(edges,jgraph.getEdgeCount());
				assertEquals(edges+1,jgraph.getVertexCount());
			}
		}
	}
	
}
