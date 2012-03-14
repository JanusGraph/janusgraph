package com.thinkaurelius.titan.graphdb.database.util;

import com.thinkaurelius.titan.core.EdgeType;
import com.thinkaurelius.titan.graphdb.edges.InternalEdge;
import com.thinkaurelius.titan.graphdb.edgetypes.EdgeTypeDefinition;
import com.thinkaurelius.titan.graphdb.edgetypes.InternalEdgeType;
import com.thinkaurelius.titan.graphdb.transaction.GraphTx;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class EdgeTypeSignature {

	private Map<String,Integer> key;
	private EdgeType[] keyET;
	
	private Map<String,Integer> value;
	private EdgeType[] valueET;
	
	public EdgeTypeSignature(EdgeType et, GraphTx tx) {
		EdgeTypeDefinition def = ((InternalEdgeType)et).getDefinition();
		key = buildIndex(def.getKeySignature());
		keyET = parseEdgeTypes(def.getKeySignature(),tx);
		value = buildIndex(def.getCompactSignature());
		valueET = parseEdgeTypes(def.getCompactSignature(),tx);
	}
	
	private static final Map<String,Integer> buildIndex(String[] strs) {
		Map<String,Integer> res = new HashMap<String,Integer>(strs.length);
		for (int i=0;i<strs.length;i++) {
			assert !res.containsKey(strs[i]);
			res.put(strs[i], Integer.valueOf(i));
		}
		return res;
	}
	
	private static final EdgeType[] parseEdgeTypes(String[] strs, GraphTx tx) {
		EdgeType[] result = new EdgeType[strs.length];
		for (int i=0;i<strs.length;i++) {
			result[i]=tx.getEdgeType(strs[i]);
		}
		return result;
	}
	
	public EdgeType getKeyEdgeType(int pos) {
		return keyET[pos];
	}
	
	public EdgeType getValueEdgeType(int pos) {
		return valueET[pos];
	}
	
	public int keyLength() {
		return key.size();
	}
	
	public int valueLength() {
		return value.size();
	}
	
	public void sort(Iterable<InternalEdge> edges, InternalEdge[] keys, InternalEdge[] values, Collection<InternalEdge> rest) {
		for (InternalEdge edge : edges) {
			String etName = edge.getEdgeType().getName();
			if (key.containsKey(etName)) {
				int pos = key.get(etName);
				assert keys[pos]==null;
				keys[pos]=edge;
			} else if (value.containsKey(etName)) {
				int pos = value.get(etName);
				assert values[pos]==null;
				values[pos]=edge;
			} else rest.add(edge);
		}
	}
	
}
