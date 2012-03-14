package com.thinkaurelius.titan.traversal;

import com.google.common.collect.ImmutableSet;
import com.thinkaurelius.titan.core.Node;
import com.thinkaurelius.titan.core.Relationship;
import edu.uci.ics.jung.graph.DirectedSparseMultigraph;
import edu.uci.ics.jung.graph.Graph;
import edu.uci.ics.jung.graph.SparseMultigraph;
import edu.uci.ics.jung.graph.UndirectedSparseMultigraph;

import java.util.Set;

/**
 * Constructs a JUNG graph from the nodes and relationships visited during traversal.
 * 
 * All the nodes and relationships that are visited during the user specified traversal operation are added to a
 * newly constructed JUNG graph. This JUNG graph can be used for further processing within the JUNG framework.
 * The JUNG framework provides functionality for graph visualization, analysis and manipulation.
 * Please see <a href="http://jung.sourceforge.net/">Java Universal Network Framework</a> for more details.
 * JUNG is a third party Java library with its own license and not part of this distribution. Please see the above
 * webpage for more information.
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 */
public class JUNGTraversal extends AbstractTraversal<JUNGTraversal> {	
	
	private ConversionMode mode;
	
	/**
	 * Constructs a new traversal to generate a JUNG graph.
	 * If directed is true, this JUNG graph will be directed, else undirected
	 *
	 */
	public JUNGTraversal(ConversionMode mode) {
		this.mode = mode;
		setParent(this);
	}
	
	/**
	 * Constructs a new traversal to generate a directed JUNG graph.
	 */
	public JUNGTraversal() {
		this(ConversionMode.Standard);
	}

	/**
	 * Constructs a JUNG graph traversing from the given set of nodes.
	 * 
	 * @param seed Set of seed nodes
	 * @return JUNG graph of the traversed subgraph
	 */
	public Graph<Node,Relationship> construct(Set<Node> seed) {
		JUNGEval eval = new JUNGEval();
		super.traversal(seed, eval);
		return eval.getGraph();
	}
	
	/**
	 * Constructs a JUNG graph traversing from the given seed node.
	 * 
	 * @param seed Seed node
	 * @return JUNG graph of the traversed subgraph
	 */
	public Graph<Node,Relationship> construct(Node seed) {
		return construct(ImmutableSet.of(seed));
	}

	private class JUNGEval implements TraversalEvaluator {

		private Graph<Node,Relationship> jgraph;
		
		private JUNGEval() {
			switch(mode) {
			case Standard:
				jgraph = new SparseMultigraph<Node,Relationship>();
				break;
			case ForceUndirected:
				jgraph = new UndirectedSparseMultigraph<Node,Relationship>();
				break;
			case IgnoreUndirected:
				jgraph = new DirectedSparseMultigraph<Node,Relationship>();	
				break;
			default: throw new IllegalArgumentException("Unsupported conversion mode: " + mode);
			}
		}
		
		@Override
		public boolean nextNode(Node node) {
			jgraph.addVertex(node);
			return true;
		}

		@Override
		public void nextRelationship(Relationship edge) {
			if (mode==ConversionMode.IgnoreUndirected && edge.isUndirected()) return;
			if (jgraph.containsEdge(edge)) return;
			
			edu.uci.ics.jung.graph.util.EdgeType et = null;
			switch(mode) {
			case Standard:
				et = getEdgeType(edge);
				break;
			case ForceUndirected:
				et = edu.uci.ics.jung.graph.util.EdgeType.UNDIRECTED;
				break;
			case IgnoreUndirected:
				et = edu.uci.ics.jung.graph.util.EdgeType.DIRECTED;
				break;
			default: throw new IllegalArgumentException("Unsupported conversion mode: " + mode);
			}
			jgraph.addEdge(edge, edge.getStart(), edge.getEnd(), et);
		}

		private Graph<Node,Relationship> getGraph() {
			return jgraph;
		}
	
	}

	public static final edu.uci.ics.jung.graph.util.EdgeType getEdgeType(Relationship edge) {
		if (edge.isDirected()) {
			return edu.uci.ics.jung.graph.util.EdgeType.DIRECTED;
		} else {
			return edu.uci.ics.jung.graph.util.EdgeType.UNDIRECTED;
		}
	}
}
