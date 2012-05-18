package com.thinkaurelius.titan.graphdb.database.util;

import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.graphdb.edges.InternalRelation;
import com.thinkaurelius.titan.graphdb.edgetypes.EdgeTypeDefinition;
import com.thinkaurelius.titan.graphdb.edgetypes.InternalTitanType;
import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class EdgeTypeSignature {

	private Map<String,Integer> key;
	private TitanType[] keyET;
	
	private Map<String,Integer> value;
	private TitanType[] valueET;
	
	public EdgeTypeSignature(TitanType et, InternalTitanTransaction tx) {
		EdgeTypeDefinition def = ((InternalTitanType)et).getDefinition();
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
	
	private static final TitanType[] parseEdgeTypes(String[] strs, InternalTitanTransaction tx) {
		TitanType[] result = new TitanType[strs.length];
		for (int i=0;i<strs.length;i++) {
			result[i]=tx.getType(strs[i]);
		}
		return result;
	}
	
	public TitanType getKeyEdgeType(int pos) {
		return keyET[pos];
	}
	
	public TitanType getValueEdgeType(int pos) {
		return valueET[pos];
	}
	
	public int keyLength() {
		return key.size();
	}
	
	public int valueLength() {
		return value.size();
	}
	
	public void sort(Iterable<InternalRelation> edges, InternalRelation[] keys, InternalRelation[] values, Collection<InternalRelation> rest) {
		for (InternalRelation edge : edges) {
			String etName = edge.getType().getName();
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
