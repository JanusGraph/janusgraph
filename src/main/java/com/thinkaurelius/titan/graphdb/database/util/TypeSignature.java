package com.thinkaurelius.titan.graphdb.database.util;

import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.graphdb.relations.InternalRelation;
import com.thinkaurelius.titan.graphdb.types.TypeDefinition;
import com.thinkaurelius.titan.graphdb.types.InternalTitanType;
import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class TypeSignature {

	private Map<String,Integer> key;
	private TitanType[] keyTypes;
	
	private Map<String,Integer> value;
	private TitanType[] valueTypes;
	
	public TypeSignature(TitanType et, InternalTitanTransaction tx) {
		TypeDefinition def = ((InternalTitanType)et).getDefinition();
		key = buildIndex(def.getKeySignature());
		keyTypes = parseTypes(def.getKeySignature(), tx);
		value = buildIndex(def.getCompactSignature());
		valueTypes = parseTypes(def.getCompactSignature(), tx);
	}
	
	private static final Map<String,Integer> buildIndex(String[] strs) {
		Map<String,Integer> res = new HashMap<String,Integer>(strs.length);
		for (int i=0;i<strs.length;i++) {
			assert !res.containsKey(strs[i]);
			res.put(strs[i], Integer.valueOf(i));
		}
		return res;
	}
	
	private static final TitanType[] parseTypes(String[] strs, InternalTitanTransaction tx) {
		TitanType[] result = new TitanType[strs.length];
		for (int i=0;i<strs.length;i++) {
			result[i]=tx.getType(strs[i]);
		}
		return result;
	}
	
	public TitanType getKeyType(int pos) {
		return keyTypes[pos];
	}
	
	public TitanType getValueType(int pos) {
		return valueTypes[pos];
	}
	
	public int keyLength() {
		return key.size();
	}
	
	public int valueLength() {
		return value.size();
	}
	
	public void sort(Iterable<InternalRelation> relations, InternalRelation[] keys, InternalRelation[] values, Collection<InternalRelation> rest) {
		for (InternalRelation relation : relations) {
			String etName = relation.getType().getName();
			if (key.containsKey(etName)) {
				int pos = key.get(etName);
				assert keys[pos]==null;
				keys[pos]=relation;
			} else if (value.containsKey(etName)) {
				int pos = value.get(etName);
				assert values[pos]==null;
				values[pos]=relation;
			} else rest.add(relation);
		}
	}
	
}
