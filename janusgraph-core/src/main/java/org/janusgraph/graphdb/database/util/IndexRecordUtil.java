// Copyright 2022 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.graphdb.database.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.JanusGraphRelation;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.JanusGraphVertexProperty;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.ReadBuffer;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.indexing.IndexEntry;
import org.janusgraph.diskstorage.indexing.StandardKeyInformation;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.util.HashingUtil;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.janusgraph.graphdb.database.EdgeSerializer;
import org.janusgraph.graphdb.database.IndexRecordEntry;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.idhandling.IDHandler;
import org.janusgraph.graphdb.database.idhandling.VariableLong;
import org.janusgraph.graphdb.database.index.IndexMutationType;
import org.janusgraph.graphdb.database.index.IndexRecords;
import org.janusgraph.graphdb.database.index.IndexUpdate;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.janusgraph.graphdb.database.serialize.DataOutput;
import org.janusgraph.graphdb.database.serialize.InternalAttributeUtil;
import org.janusgraph.graphdb.database.serialize.Serializer;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.internal.InternalRelationType;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.janusgraph.graphdb.olap.QueryContainer;
import org.janusgraph.graphdb.query.vertex.VertexCentricQueryBuilder;
import org.janusgraph.graphdb.relations.RelationIdentifier;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.types.CompositeIndexType;
import org.janusgraph.graphdb.types.IndexField;
import org.janusgraph.graphdb.types.IndexType;
import org.janusgraph.graphdb.types.MixedIndexType;
import org.janusgraph.graphdb.types.ParameterIndexField;
import org.janusgraph.graphdb.types.ParameterType;
import org.janusgraph.graphdb.types.TypeInspector;
import org.janusgraph.util.IDUtils;
import org.janusgraph.util.encoding.LongEncoding;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.janusgraph.util.encoding.LongEncoding.STRING_ENCODING_MARKER;

public class IndexRecordUtil {

    public static final IndexAppliesToFunction FULL_INDEX_APPLIES_TO_FILTER = IndexRecordUtil::indexAppliesTo;
    public static final IndexAppliesToFunction INDEX_APPLIES_TO_NO_CONSTRAINTS_FILTER = IndexRecordUtil::indexAppliesToWithoutConstraints;

    private static final int DEFAULT_OBJECT_BYTELEN = 30;
    private static final byte FIRST_INDEX_COLUMN_BYTE = 0;

    public static Object[] getValues(IndexRecordEntry[] record) {
        final Object[] values = new Object[record.length];
        for (int i = 0; i < values.length; i++) {
            values[i]=record[i].getValue();
        }
        return values;
    }

    public static MixedIndexType getMixedIndex(String indexName, StandardJanusGraphTx transaction) {
        final IndexType index = ManagementSystem.getGraphIndexDirect(indexName, transaction);
        Preconditions.checkArgument(index!=null,"Index with name [%s] is unknown or not configured properly",indexName);
        Preconditions.checkArgument(index.isMixedIndex());
        return (MixedIndexType)index;
    }

    public static String element2String(JanusGraphElement element) {
        return element2String(element.id());
    }

    /**
     * Convert an element's (including vertex and relation) id into a String
     *
     * @param elementId
     * @return
     */
    public static String element2String(Object elementId) {
        Preconditions.checkArgument(elementId instanceof Long || elementId instanceof RelationIdentifier || elementId instanceof String);
        if (elementId instanceof RelationIdentifier) {
            return ((RelationIdentifier) elementId).toString();
        } else {
            return id2Name(elementId);
        }
    }

    public static Object string2ElementId(String str) {
        if (StringUtils.isEmpty(str)) {
            throw new IllegalArgumentException("Empty string cannot be converted to a valid id");
        }
        if (str.contains(RelationIdentifier.TOSTRING_DELIMITER)) {
            return RelationIdentifier.parse(str);
        } else {
            return name2Id(str);
        }
    }

    public static String key2Field(MixedIndexType index, PropertyKey key) {
        return key2Field(index.getField(key));
    }

    public static String key2Field(ParameterIndexField field) {
        assert field!=null;
        return ParameterType.MAPPED_NAME.findParameter(field.getParameters(),keyID2Name(field.getFieldKey()));
    }

    public static String keyID2Name(PropertyKey key) {
        return id2Name(key.longId());
    }

    public static String id2Name(Object id) {
        IDUtils.checkId(id);
        if (id instanceof Number) {
            return LongEncoding.encode(((Number) id).longValue());
        } else {
            return STRING_ENCODING_MARKER + id.toString();
        }
    }

    public static Object name2Id(String name) {
        if (name.charAt(0) == STRING_ENCODING_MARKER) {
            return name.substring(1);
        } else {
            return LongEncoding.decode(name);
        }
    }

    public static RelationIdentifier bytebuffer2RelationId(ReadBuffer b) {
        Object[] relationId = new Object[4];
        relationId[0] = VariableLong.readPositive(b);
        relationId[1] = IDHandler.readVertexId(b, true);
        relationId[2] = VariableLong.readPositive(b);
        if (b.hasRemaining()) {
            relationId[3] = IDHandler.readVertexId(b, true);
        } else {
            relationId = Arrays.copyOfRange(relationId,0,3);
        }
        return RelationIdentifier.get(relationId);
    }

    public static StandardKeyInformation getKeyInformation(final ParameterIndexField field) {
        return new StandardKeyInformation(field.getFieldKey(),field.getParameters());
    }

    public static IndexMutationType getUpdateType(InternalRelation relation, boolean isInlined) {
        assert relation.isNew() || relation.isRemoved();
        return isInlined ? IndexMutationType.UPDATE : (relation.isNew() ? IndexMutationType.ADD : IndexMutationType.DELETE);
    }

    public static boolean indexAppliesTo(IndexType index, JanusGraphElement element) {
        return indexAppliesToWithoutConstraints(index, element) && indexMatchesConstraints(index, element);
    }

    public static boolean indexAppliesToWithoutConstraints(IndexType index, JanusGraphElement element) {
        return index.getElement().isInstance(element) &&
            (!(index instanceof CompositeIndexType) || ((CompositeIndexType)index).getStatus()!= SchemaStatus.DISABLED);
    }

    public static boolean indexMatchesConstraints(IndexType index, JanusGraphElement element) {
        return !index.hasSchemaTypeConstraint() ||
            index.getElement().matchesConstraint(index.getSchemaTypeConstraint(),element);
    }

    public static PropertyKey[] getKeysOfRecords(IndexRecordEntry[] record) {
        final PropertyKey[] keys = new PropertyKey[record.length];
        for (int i=0;i<record.length;i++) keys[i]=record[i].getKey();
        return keys;
    }

    public static int getIndexTTL(InternalVertex vertex, PropertyKey... keys) {
        int ttl = StandardJanusGraph.getTTL(vertex);
        for (final PropertyKey key : keys) {
            final int kttl = ((InternalRelationType) key).getTTL();
            if (kttl > 0 && (kttl < ttl || ttl <= 0)) ttl = kttl;
        }
        return ttl;
    }

    public static IndexRecordEntry[] indexMatch(JanusGraphRelation relation, CompositeIndexType index) {
        final IndexField[] fields = index.getFieldKeys();
        final IndexRecordEntry[] match = new IndexRecordEntry[fields.length];
        for (int i = 0; i <fields.length; i++) {
            final IndexField f = fields[i];
            final Object value = relation.valueOrNull(f.getFieldKey());
            if (value==null) return null; //No match
            match[i] = new IndexRecordEntry(relation.longId(),value,f.getFieldKey());
        }
        return match;
    }

    public static IndexRecords indexMatches(JanusGraphVertex vertex, CompositeIndexType index) {
        return indexMatches(vertex,index,null,null);
    }

    public static IndexRecords indexMatches(JanusGraphVertex vertex, CompositeIndexType index,
                                            PropertyKey replaceKey, Object replaceValue) {
        final IndexRecords matches = new IndexRecords();
        final IndexField[] fields = index.getFieldKeys();
        if (indexAppliesTo(index,vertex)) {
            indexMatches(vertex,new IndexRecordEntry[fields.length],matches,fields,0,false,
                replaceKey,new IndexRecordEntry(0,replaceValue,replaceKey));
        }
        return matches;
    }

    public static IndexRecords indexMatches(JanusGraphVertex vertex, CompositeIndexType index,
                                             boolean onlyLoaded, PropertyKey replaceKey, IndexRecordEntry replaceValue) {
        final IndexRecords matches = new IndexRecords();
        final IndexField[] fields = index.getFieldKeys();
        indexMatches(vertex,new IndexRecordEntry[fields.length],matches,fields,0,onlyLoaded,replaceKey,replaceValue);
        return matches;
    }

    public static void indexMatches(JanusGraphVertex vertex, IndexRecordEntry[] current, IndexRecords matches,
                                     IndexField[] fields, int pos,
                                     boolean onlyLoaded, PropertyKey replaceKey, IndexRecordEntry replaceValue) {
        if (pos>= fields.length) {
            matches.add(current);
            return;
        }

        final PropertyKey key = fields[pos].getFieldKey();

        List<IndexRecordEntry> values;
        if (key.equals(replaceKey)) {
            values = ImmutableList.of(replaceValue);
        } else {
            values = new ArrayList<>();
            Iterator<JanusGraphVertexProperty> props;
            if (onlyLoaded ||
                (!vertex.isNew() && IDManager.VertexIDType.PartitionedVertex.is(vertex.id()))) {
                //going through transaction so we can query deleted vertices
                final VertexCentricQueryBuilder qb = ((InternalVertex)vertex).tx().query(vertex);
                qb.noPartitionRestriction().type(key);
                if (onlyLoaded) qb.queryOnlyLoaded();
                props = qb.properties().iterator();
            } else {
                props = Iterators.transform(vertex.properties(key.name()), i -> (JanusGraphVertexProperty) i);
            }
            while (props.hasNext()) {
                JanusGraphVertexProperty p = props.next();
                assert !onlyLoaded || p.isLoaded() || p.isRemoved();
                assert key.dataType().equals(p.value().getClass()) : key + " -> " + p;
                values.add(new IndexRecordEntry(p));
            }
        }
        for (final IndexRecordEntry value : values) {
            current[pos]=value;
            indexMatches(vertex,current,matches,fields,pos+1,onlyLoaded,replaceKey,replaceValue);
        }
    }


    private static Entry getIndexEntry(CompositeIndexType index, IndexRecordEntry[] record,
                                       JanusGraphElement element,
                                       Serializer serializer,
                                       TypeInspector typeInspector,
                                       EdgeSerializer edgeSerializer) {

        List<Entry> inlineProperties = getInlineProperties(element, index, typeInspector, edgeSerializer);
        int inlinePropertiesSize = getInlinePropertiesSize(inlineProperties);

        final DataOutput out = serializer.getDataOutput(1 + 8 + 8 * record.length + 4 * 8 + inlinePropertiesSize);
        out.putByte(FIRST_INDEX_COLUMN_BYTE);
        if (index.getCardinality() != Cardinality.SINGLE) {
            if (element instanceof JanusGraphVertex) {
                IDHandler.writeVertexId(out, element.id(), true);
            } else {
                assert element instanceof JanusGraphRelation;
                assert ((JanusGraphRelation) element).longId() == ((RelationIdentifier) element.id()).getRelationId();
                VariableLong.writePositive(out, ((JanusGraphRelation) element).longId());
            }
            if (index.getCardinality() != Cardinality.SET) {
                for (final IndexRecordEntry re : record) {
                    VariableLong.writePositive(out, re.getRelationId());
                }
            }
        }
        final int valuePosition = out.getPosition();
        if (element instanceof JanusGraphVertex) {
            IDHandler.writeVertexId(out, element.id(), true);
            writeInlineProperties(inlineProperties, out);
        } else {
            assert element instanceof JanusGraphRelation;
            final RelationIdentifier rid = (RelationIdentifier) element.id();
            VariableLong.writePositive(out, rid.getRelationId());
            IDHandler.writeVertexId(out, rid.getOutVertexId(), true);
            VariableLong.writePositive(out, rid.getTypeId());
            if (rid.getInVertexId() != null) {
                IDHandler.writeVertexId(out, rid.getInVertexId(), true);
            }
        }
        return new StaticArrayEntry(out.getStaticBuffer(), valuePosition);
    }

    public static StaticBuffer getIndexKey(CompositeIndexType index, IndexRecordEntry[] record, Serializer serializer, boolean hashKeys, HashingUtil.HashLength hashLength) {
        return getIndexKey(index, IndexRecordUtil.getValues(record), serializer, hashKeys, hashLength);
    }

    public static StaticBuffer getIndexKey(CompositeIndexType index, Object[] values, Serializer serializer, boolean hashKeys, HashingUtil.HashLength hashLength) {
        final DataOutput out = serializer.getDataOutput(8*DEFAULT_OBJECT_BYTELEN + 8);
        VariableLong.writePositive(out, index.longId());
        final IndexField[] fields = index.getFieldKeys();
        Preconditions.checkArgument(fields.length>0 && fields.length==values.length);
        for (int i = 0; i < fields.length; i++) {
            final IndexField f = fields[i];
            final Object value = values[i];
            Preconditions.checkNotNull(value);
            if (InternalAttributeUtil.hasGenericDataType(f.getFieldKey())) {
                out.writeClassAndObject(value);
            } else {
                assert value.getClass().equals(f.getFieldKey().dataType()) : value.getClass() + " - " + f.getFieldKey().dataType();
                out.writeObjectNotNull(value);
            }
        }
        StaticBuffer key = out.getStaticBuffer();
        if (hashKeys) key = HashingUtil.hashPrefixKey(hashLength,key);
        return key;
    }

    public static long getIndexIdFromKey(StaticBuffer key, boolean hashKeys, HashingUtil.HashLength hashLength) {
        if (hashKeys) key = HashingUtil.getKey(hashLength,key);
        return VariableLong.readPositive(key.asReadBuffer());
    }

    public static IndexUpdate<StaticBuffer, Entry> getCompositeIndexUpdate(CompositeIndexType index, IndexMutationType indexMutationType, IndexRecordEntry[] record,
                                                                           JanusGraphElement element,
                                                                           Serializer serializer,
                                                                           TypeInspector typeInspector,
                                                                           EdgeSerializer edgeSerializer,
                                                                           boolean hashKeys,
                                                                           HashingUtil.HashLength hashLength){
        return new IndexUpdate<>(index, indexMutationType,
            getIndexKey(index, record, serializer, hashKeys, hashLength),
            getIndexEntry(index, record, element, serializer, typeInspector, edgeSerializer), element);
    }

    public static IndexUpdate<String, IndexEntry> getMixedIndexUpdate(JanusGraphElement element, PropertyKey key, Object value,
                                                                      MixedIndexType index, IndexMutationType updateType)  {
        return new IndexUpdate<>(index, updateType, element2String(element), new IndexEntry(key2Field(index.getField(key)), value), element);
    }

    public static int getInlinePropertiesSize(List<Entry> inlineProperties) {
        return inlineProperties.size() * Integer.BYTES * 2 + inlineProperties.stream().mapToInt(StaticBuffer::length).sum();
    }

    public static void writeInlineProperties(List<Entry> inlineProperties, DataOutput out) {
        inlineProperties.forEach(entry -> {
            out.putInt(entry.length());
            out.putInt(entry.getValuePosition());
            out.putBytes(entry);
        });
    }

    public static Iterable<Entry> readInlineProperties(ReadBuffer readBuffer) {

        return () -> new Iterator<Entry>() {
            @Override
            public boolean hasNext() {
                return readBuffer.hasRemaining();
            }

            @Override
            public Entry next() {
                int entryDataSize = readBuffer.getInt();
                int valuePos = readBuffer.getInt();
                byte[] entryBytes = readBuffer.getBytes(entryDataSize);
                return new StaticArrayEntry(entryBytes, valuePos);
            }
        };
    }

    public static List<Entry> getInlineProperties(JanusGraphElement element,
                                                  CompositeIndexType index,
                                                  TypeInspector typeInspector,
                                                  EdgeSerializer edgeSerializer) {
        if (element instanceof JanusGraphVertex && index.getInlineFieldKeys().length != 0 && !element.isRemoved()) {
            Iterator<VertexProperty<Object>> props = ((JanusGraphVertex) element).properties(index.getInlineFieldKeys());
            return IteratorUtils.list(IteratorUtils.map(props,
                prop -> edgeSerializer.writeRelation((InternalRelation) prop, 0, typeInspector)));
        } else {
            return Collections.emptyList();
        }
    }

    public static Map<String, SliceQuery> getInlinePropertiesQueries(CompositeIndexType index, StandardJanusGraphTx tx) {
        if (index.getInlineFieldKeys().length == 0) {
            return Collections.emptyMap();
        } else {

            Map<String, SliceQuery> result = new HashMap<>(index.getInlineFieldKeys().length);
            for(String inlineKey: index.getInlineFieldKeys()) {
                QueryContainer qc = new QueryContainer(tx);
                qc.addQuery().direction(Direction.OUT).keys(inlineKey).properties();
                List<SliceQuery> sliceQueries = qc.getSliceQueries();
                assert sliceQueries.size() == 1;
                result.put(inlineKey, sliceQueries.get(0));
            }
            return result;
        }
    }
}
