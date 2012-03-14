package com.thinkaurelius.titan.graphdb.loading.test;

import com.thinkaurelius.titan.core.Direction;
import com.thinkaurelius.titan.graphdb.edgequery.StandardEdgeQuery;
import com.thinkaurelius.titan.graphdb.edgetypes.system.SystemRelationshipType;
import com.thinkaurelius.titan.graphdb.loadingstatus.DefaultLoadingStatus;
import com.thinkaurelius.titan.graphdb.loadingstatus.LoadingStatus;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EdgeLoadingTest {

	@Test
	public void edgeLoadingTest() {
		LoadingStatus status1 = DefaultLoadingStatus.AllLoaded;
		LoadingStatus status2 = DefaultLoadingStatus.NothingLoaded;
		StandardEdgeQuery q = new StandardEdgeQuery(null,1);
		assertTrue(status1.hasLoadedEdges(q));
		assertFalse(status2.hasLoadedEdges(q));
		assertTrue(status1==status1.loadedEdges(q));
		assertFalse(status2==status2.loadedEdges(q));
		
		LoadingStatus status=status2;
		assertTrue(status.loadedEdges(q).hasLoadedEdges(q));
		q.inDirection(Direction.Out);
		assertFalse(status.hasLoadedEdges(q));
		status2 = status.loadedEdges(q);
		assertTrue(status2.hasLoadedEdges(q));
		q.withEdgeType(SystemRelationshipType.EdgeType);
		assertTrue(status2.hasLoadedEdges(q));
		assertFalse(status.hasLoadedEdges(q));
	}
	
}
