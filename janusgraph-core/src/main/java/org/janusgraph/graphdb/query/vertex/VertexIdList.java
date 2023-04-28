// Copyright 2017 JanusGraph Authors
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

package org.janusgraph.graphdb.query.vertex;

import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.VertexList;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.util.IDUtils;
import org.janusgraph.util.datastructures.AbstractIdListUtil;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * An implementation of {@link VertexListInternal} that stores only the vertex ids
 * and simply wraps an {@link List} and keeps a boolean flag to remember whether this list is in sort order.
 * In addition, we need a transaction reference in order to construct actual vertex references on request.
 * <p>
 * This is a more efficient way to represent a vertex result set but only applies to loaded vertices that have ids.
 * So, compared to {@link VertexArrayList} this is an optimization for the special use case that a vertex is loaded.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class VertexIdList implements VertexListInternal {

    public static final Comparator<Object> VERTEX_ID_COMPARATOR = IDUtils::compare;
    private final StandardJanusGraphTx tx;
    private List<Object> vertices;
    private boolean sorted;

    public VertexIdList(StandardJanusGraphTx tx) {
        this(tx,new ArrayList<>(10),true);
    }

    public VertexIdList(StandardJanusGraphTx tx, List<Object> vertices, boolean sorted) {
        assert !sorted || AbstractIdListUtil.isSorted(vertices);
        this.tx = tx;
        this.vertices = vertices;
        this.sorted = sorted;
    }

    @Override
    public void add(JanusGraphVertex n) {
        if (!vertices.isEmpty()) {
            sorted = sorted && (IDUtils.compare(vertices.get(vertices.size() - 1), n.id()) <= 0);
        }
        vertices.add(n.id());
    }

    @Override
    public Object getID(int pos) {
        return vertices.get(pos);
    }

    @Override
    public List<Object> getIDs() {
        return vertices;
    }

    @Override
    public JanusGraphVertex get(int pos) {
        return tx.getInternalVertex(getID(pos));
    }

    @Override
    public void sort() {
        if (sorted) return;
        vertices.sort(VERTEX_ID_COMPARATOR);
        sorted = true;
    }

    @Override
    public boolean isSorted() {
        return sorted;
    }

    @Override
    public VertexList subList(int fromPosition, int length) {
        List<Object> subList = new ArrayList<>(length);
        subList.addAll(vertices.subList(fromPosition, fromPosition + length));
        assert subList.size()==length;
        return new VertexIdList(tx,subList,sorted);
    }

    @Override
    public int size() {
        return vertices.size();
    }

    @Override
    public void addAll(VertexList vertexlist) {
        final List<Object> otherVertexIds;
        if (vertexlist instanceof VertexIdList) {
            otherVertexIds = ((VertexIdList) vertexlist).vertices;
        } else if (vertexlist instanceof VertexArrayList) {
            VertexArrayList other = (VertexArrayList) vertexlist;
            otherVertexIds = new ArrayList<>(other.size());
            for (int i = 0; i < other.size(); i++) otherVertexIds.add(other.getID(i));
        } else {
            throw new IllegalArgumentException("Unsupported vertex-list: " + vertexlist.getClass());
        }
        if (sorted && vertexlist.isSorted()) {
            vertices = AbstractIdListUtil.mergeSort(vertices,otherVertexIds);
        } else {
            sorted = false;
            vertices.addAll(otherVertexIds);
        }
    }

    public VertexArrayList toVertexArrayList() {
        VertexArrayList list = new VertexArrayList(tx);
        for (int i=0;i<vertices.size();i++) {
            list.add(get(i));
        }
        return list;
    }

    @Override
    public Iterator<JanusGraphVertex> iterator() {
        return new Iterator<JanusGraphVertex>() {

            private int pos = -1;

            @Override
            public boolean hasNext() {
                return (pos + 1) < size();
            }

            @Override
            public JanusGraphVertex next() {
                if (!hasNext()) throw new NoSuchElementException();
                pos++;
                return get(pos);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Vertices cannot be removed from neighborhood list");
            }

        };
    }

}
