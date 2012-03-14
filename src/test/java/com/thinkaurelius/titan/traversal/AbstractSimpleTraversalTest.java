package com.thinkaurelius.titan.traversal;

import com.google.common.collect.ImmutableSet;
import com.thinkaurelius.titan.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.test.AbstractGraphDBTestCommon;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public abstract class AbstractSimpleTraversalTest
	extends AbstractGraphDBTestCommon {
	
	protected PropertyType id;
	protected RelationshipType a2a, a2b, b2c;
	protected EdgeTypeGroup abRel, allRel;
	
	protected static final int CHAIN_LENGTH = 1024;
	
	protected AbstractSimpleTraversalTest(GraphDatabaseConfiguration config) {
		super(config);
	}

	@Before
	public void setUp() throws Exception {
		super.setUp();
		
		/* Generate a simple synthetic graph.
		 * The graph has three reltypes and is acyclic.
		 * A rough sketch of the graph structure:
		 * 
		 * a0 -> b0 -> c0
		 * |
		 * a1
		 * |
		 * a2 -> b2
		 * |
		 * a3
		 * |
		 * a4 -> b4 -> c4
		 * |
		 * a5
		 * |
		 * a6 -> b6
		 * |
		 * a7
		 * |
		 * a8 -> b8 -> c8
		 * |
		 * ... (numerical part limited by CHAIN_LENTH constant)
		 * 
		 * The reltype linking aX to aY is "a2a" (undirected).
		 * The reltype linking aX -> bX is "a2b" (directed).
		 * The reltype linking bX -> cX is "b2c" (directed).
		 * 
		 * The edgetype group "abRel" contains reltype "a2a" and "a2b".
		 * 
		 * The only propertytype is "id". Its value is "a0", "b0", etc.
		 * 
		 */
		id = makeStringIDPropertyType("id");
		abRel = EdgeTypeGroup.of(6, "abRel");
		a2a = makeRelationshipType("a2a", abRel, Directionality.Undirected);
		a2b = makeRelationshipType("a2b", abRel, Directionality.Directed);
		b2c = makeRelationshipType("b2c", EdgeTypeGroup.DefaultGroup, Directionality.Directed);
		Node prevA = null;
		for (int i = 0; i < CHAIN_LENGTH; i++) {
			Node a = tx.createNode();
			a.createProperty(id, "a" + i);
			if (0 == i % 2) {
				// Create "b" and link a to it
				Node b = tx.createNode();
				b.createProperty(id, "b" + i);
				a.createRelationship(a2b, b);

				if (0 == i % 4) {
					// create "c" and link b to it
					Node c = tx.createNode();
					c.createProperty(id, "c" + i);
					b.createRelationship(b2c, c);
				}
			}
			// Link current "a" node to previous one
			if (null != prevA) {
				prevA.createRelationship(a2a, a);
			}
			prevA = a;
		}
		clopen();
		id = tx.getPropertyType("id");
		a2a = tx.getRelationshipType("a2a");
		a2b = tx.getRelationshipType("a2b");
		b2c = tx.getRelationshipType("b2c");
		
	}

	@Test
	public void depthZeroTraversalFromMultipleSeedsJustReturnsSeeds() {
		// Test traversing depth=0 from multiple seeds
		List<String> seedIds = Arrays.asList("a32", "a64", "a96");
		Set<String> expectedNodeIds = ImmutableSet.copyOf(seedIds);
		Set<Node> seeds = new HashSet<Node>();
		for (String s : seedIds) {
			seeds.add(tx.getNodeByKey(id, s));
		}
		NodeTraversal ntrav = new NodeTraversal();
		ntrav.setDepth(0);
		Set<String> travNodeIds = mapNodeSetToNodeIdSet(ntrav.traversal(seeds));
		assertEquals(expectedNodeIds, travNodeIds);
	}
	
	@Test
	public void depthOneTraversalWithTwoEdgeTypes() {
		/* Test depth=1 traversal from a single seed on a directed edgetype
		 * and an undirected edgetype
		 */
		Node seed = tx.getNodeByKey(id, "a128");
		Set<String> expectedNodeIds = ImmutableSet.of("a128", "a127", "a129", "b128");
		NodeTraversal ntrav = new NodeTraversal();
		ntrav.addRelationshipType(a2a);
		ntrav.addRelationshipType(a2b);
		ntrav.setDepth(1);
		Set<String> travNodeIds = mapNodeSetToNodeIdSet(ntrav.traversal(seed));
		assertEquals(expectedNodeIds, travNodeIds);
	}
	
	@Test
	public void depthOneTraversalWithTwoDirectedEdgeTypes() {
		Node seed = tx.getNodeByKey(id, "a128");
		Set<String> expectedNodeIds = ImmutableSet.of("a128", "b128");
		NodeTraversal ntrav = new NodeTraversal();
		ntrav.addRelationshipType(a2b, Direction.Out);
		ntrav.addRelationshipType(b2c, Direction.Out);
		ntrav.setDepth(1);
		Set<String> travNodeIds = mapNodeSetToNodeIdSet(ntrav.traversal(seed));
		assertEquals(expectedNodeIds, travNodeIds);
	}
	
	@Test
	public void depthExcessiveTraversalOverTwoNodes() {
		// Test case with excessive depth limit and directed edges
		Node seed = tx.getNodeByKey(id, "a128");
		NodeTraversal ntrav = new NodeTraversal();
		ntrav.addRelationshipType(a2b, Direction.Out);
		ntrav.addRelationshipType(b2c, Direction.Out);
		ntrav.setDepth(CHAIN_LENGTH * 4);
		Set<String >expectedNodeIds = ImmutableSet.of("a128", "b128", "c128");
		Set<String> travNodeIds = mapNodeSetToNodeIdSet(ntrav.traversal(seed));
		assertEquals(expectedNodeIds, travNodeIds);
	}
	
	@Test
	public void depthFourMultipleSeedTraversal() {
		// Test multiple-seed traversal with depth=4 over all edge types
		Set<String> expectedNodeIds = new HashSet<String>();
		List<Integer> seedNumbers = Arrays.asList(32, 64, 96);
		Set<Node> seeds = new HashSet<Node>();
		for (Integer seedNumber : seedNumbers) {
			seeds.add(tx.getNodeByKey(id, "a" + seedNumber));
			
			expectedNodeIds.add("a" + (seedNumber - 4));
			expectedNodeIds.add("a" + (seedNumber - 3));
			expectedNodeIds.add("b" + (seedNumber - 2));
			expectedNodeIds.add("a" + (seedNumber - 2));
			expectedNodeIds.add("a" + (seedNumber - 1));
			expectedNodeIds.add("a" + seedNumber);
			expectedNodeIds.add("b" + seedNumber);
			expectedNodeIds.add("c" + seedNumber);
			expectedNodeIds.add("a" + (seedNumber + 1));
			expectedNodeIds.add("a" + (seedNumber + 2));
			expectedNodeIds.add("b" + (seedNumber + 2));
			expectedNodeIds.add("a" + (seedNumber + 3));
			expectedNodeIds.add("a" + (seedNumber + 4));
		}
		NodeTraversal ntrav = new NodeTraversal();
		ntrav.addRelationshipType(a2a, Direction.Undirected);
		ntrav.addRelationshipType(a2b, Direction.Out);
		ntrav.addRelationshipType(b2c, Direction.Out);
		ntrav.setDepth(4);
		Set<String> travNodeIds = mapNodeSetToNodeIdSet(ntrav.traversal(seeds));
		assertEquals(expectedNodeIds, travNodeIds);
	}

	@Test
	public void traverseEntireGraphFromSingleSeed() {
		// Test traversing whole graph
		Node seed = tx.getNodeByKey(id, "a0");
		Set<String> expectedNodeIds = new HashSet<String>();
		for (int i = 0; i < CHAIN_LENGTH; i++) {
			expectedNodeIds.add("a" + i);
			if (0 == i % 2)
				expectedNodeIds.add("b" + i);
			if (0 == i % 4)
				expectedNodeIds.add("c" + i);
		}
		NodeTraversal ntrav = new NodeTraversal();
		ntrav.addRelationshipType(a2a, Direction.Undirected);
		ntrav.addRelationshipType(a2b, Direction.Out);
		ntrav.addRelationshipType(b2c, Direction.Out);
		ntrav.setDepth(CHAIN_LENGTH * 4);
		Set<String> travNodeIds = mapNodeSetToNodeIdSet(ntrav.traversal(seed));
		assertEquals(expectedNodeIds, travNodeIds);
	}
	
	@Test
	public void traverseEdgeTypeGroupIgnoresNonMemberEdgeType() {
		Node seed = tx.getNodeByKey(id, "a512");
		Set<String> expectedNodeIds = ImmutableSet.of("a512", "b512");
		NodeTraversal ntrav = new NodeTraversal();
		ntrav.addEdgeTypeGroup(abRel, Direction.Both);
		ntrav.setDepth(2);
		Set<String> travNodeIds = mapNodeSetToNodeIdSet(ntrav.traversal(seed));
		assertEquals(expectedNodeIds, travNodeIds);
	}
	
	@Test
	public void traverseCombinationOfRelationshipTypeAndEdgeTypeGroup() {
		/* Test that NodeTraversal considers the union of 
		 * addRelationshipType() and addEdgeTypeGroup()
		 */
		Node seed = tx.getNodeByKey(id, "a512");
		Set<String> expectedNodeIds = ImmutableSet.of(
				"a510", "a511", "a512", "b512", "c512", "a513", "a514");
		NodeTraversal ntrav = new NodeTraversal();
		ntrav.addEdgeTypeGroup(abRel, Direction.Both);
		ntrav.addRelationshipType(a2a, Direction.Undirected);
		ntrav.addRelationshipType(b2c, Direction.Out);
		ntrav.setDepth(2);
		Set<String> travNodeIds = mapNodeSetToNodeIdSet(ntrav.traversal(seed));
		assertEquals(expectedNodeIds, travNodeIds);
	}
	
	@Test
	public void traversalAgainstEdgeTypeDirection() {
		Node seed = tx.getNodeByKey(id, "c64");
		Set<String> expectedNodeIds = ImmutableSet.of("c64", "b64", "a64", "a63", "a65");
		NodeTraversal ntrav = new NodeTraversal();
		ntrav.addRelationshipType(a2a, Direction.Undirected);
		ntrav.addRelationshipType(a2b, Direction.In);
		ntrav.addRelationshipType(b2c, Direction.In);
		ntrav.setDepth(3);
		Set<String> travNodeIds = mapNodeSetToNodeIdSet(ntrav.traversal(seed));
		assertEquals(expectedNodeIds, travNodeIds);
	}
	
	@Test
	public void traversalDirectionBothMatchesDirectionInOrDirectionOut() {
		Set<String> expectedNodeIds = ImmutableSet.of("c0", "b0", "a0");
		NodeTraversal ntrav = new NodeTraversal();
		ntrav.addRelationshipType(a2b, Direction.Both);
		ntrav.addRelationshipType(b2c, Direction.Both);
		ntrav.setDepth(3);
		for (String seedName : Arrays.asList("a0", "b0", "c0")) {
			Node s = tx.getNodeByKey(id, seedName);
			Set<String> travNodeIds = mapNodeSetToNodeIdSet(ntrav.traversal(s));
			assertEquals(expectedNodeIds, travNodeIds);
		}
	}
	
	@Test
	public void traversalDirectionBothDoesNotMatchUndirected() {
		Set<String> expectedNodeIds = ImmutableSet.of("a10");
		Node seed = tx.getNodeByKey(id, "a10");
		NodeTraversal ntrav = new NodeTraversal();
		ntrav.addRelationshipType(b2c, Direction.Both); // should get no matches
		ntrav.setDepth(50);
		Set<String> travNodeIds = mapNodeSetToNodeIdSet(ntrav.traversal(seed));
		assertEquals(expectedNodeIds, travNodeIds);
	}
	
	private Set<String> mapNodeSetToNodeIdSet(Set<Node> nodes) {
		Set<String> ids = new HashSet<String>(nodes.size());
		PropertyType idProp = tx.getPropertyType("id");
		for (Node n : nodes) {
			String id = n.getString(idProp);
			assertFalse(ids.contains(id));
			ids.add(id);
		}
		assertEquals(nodes.size(), ids.size());
		return ids;
	}
}
