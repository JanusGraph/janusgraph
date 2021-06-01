// Copyright 2019 JanusGraph Authors
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

package org.janusgraph.graphdb.database;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.indexing.IndexEntry;
import org.janusgraph.diskstorage.indexing.IndexInformation;
import org.janusgraph.graphdb.database.serialize.Serializer;
import org.janusgraph.graphdb.internal.ElementCategory;
import org.janusgraph.graphdb.internal.ElementLifeCycle;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.types.MixedIndexType;
import org.janusgraph.graphdb.types.ParameterIndexField;
import org.janusgraph.graphdb.vertices.StandardVertex;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

public class IndexSerializerTest {

    @Test
    public void testReindexElementNotAppliesTo() {
        Configuration config = mock(Configuration.class);
        Serializer serializer = mock(Serializer.class);
        Map<String, ? extends IndexInformation> indexes = new HashMap<>();

        IndexSerializer mockSerializer = new IndexSerializer(config, serializer, indexes, true);
        JanusGraphElement nonIndexableElement = mock(JanusGraphElement.class);
        MixedIndexType mit = mock(MixedIndexType.class);
        doReturn(ElementCategory.VERTEX).when(mit).getElement();

        Map<String, Map<String, List<IndexEntry>>> docStore = new HashMap<>();
        assertFalse("re-index", mockSerializer.reindexElement(nonIndexableElement, mit, docStore));

    }

    @Test
    public void testReindexElementAppliesToWithEntries() {
        Map<String, Map<String, List<IndexEntry>>> docStore = new HashMap<>();
        IndexSerializer mockSerializer = mockSerializer();
        MixedIndexType mit = mock(MixedIndexType.class);

        JanusGraphElement indexableElement = mockIndexAppliesTo(mit, true);

        assertTrue("re-index", mockSerializer.reindexElement(indexableElement, mit, docStore));
        assertEquals("doc store size", 1, docStore.size());

    }

    @Test
    public void testReindexElementAppliesToNoEntries() {
        Map<String, Map<String, List<IndexEntry>>> docStore = new HashMap<>();
        IndexSerializer mockSerializer = mockSerializer();
        MixedIndexType mit = mock(MixedIndexType.class);
        JanusGraphElement indexableElement = mockIndexAppliesTo(mit, false);

        assertFalse("re-index", mockSerializer.reindexElement(indexableElement, mit, docStore));
        assertEquals("doc store size", 0, docStore.size());

    }

    private IndexSerializer mockSerializer() {
        Configuration config = mock(Configuration.class);
        Serializer serializer = mock(Serializer.class);
        Map<String, ? extends IndexInformation> indexes = new HashMap<>();
        return spy(new IndexSerializer(config, serializer, indexes, true));
    }

    private JanusGraphElement mockIndexAppliesTo(MixedIndexType mit, boolean indexable) {
        String key = "foo";
        String value = "bar";

        JanusGraphElement indexableElement = mockIndexableElement(key, value, indexable);
        ElementCategory ec = ElementCategory.VERTEX;

        doReturn(ec).when(mit).getElement();

        doReturn(false).when(mit).hasSchemaTypeConstraint();

        PropertyKey pk = mock(PropertyKey.class);
        doReturn(1L).when(pk).longId();
        doReturn(key).when(pk).name();
        ParameterIndexField pif = mock(ParameterIndexField.class);
        Parameter[] parameter = { new Parameter(key, value) };
        doReturn(parameter).when(pif).getParameters();
        doReturn(SchemaStatus.REGISTERED).when(pif).getStatus();
        doReturn(pk).when(pif).getFieldKey();

        ParameterIndexField[] ifField = { pif };

        doReturn(ifField).when(mit).getFieldKeys();

        return indexableElement;

    }

    private JanusGraphElement mockIndexableElement(String key, String value, boolean indexable) {
        StandardJanusGraphTx tx = mock(StandardJanusGraphTx.class);
        JanusGraphElement indexableElement = spy(new StandardVertex(tx, 1L, ElementLifeCycle.New));
        Property pk2 = indexableElement.property(key, value);
        Iterator it = Arrays.asList(pk2).iterator();
        doReturn(it).when(indexableElement).properties(key);
        if (indexable)
            doReturn(Arrays.asList(value).iterator()).when(indexableElement).values(key);
        else
            doReturn(new ArrayList<>().iterator()).when(indexableElement).values(key); // skpping the values section!!

        return indexableElement;

    }

}
