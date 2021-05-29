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

package org.janusgraph.graphdb.internal;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.relations.RelationIdentifier;

/**
 * AbstractElement is the base class for all elements in JanusGraph.
 * It is defined and uniquely identified by its id.
 * <p>
 * For the id, it holds that:
 * id&lt;0: Temporary id, will be assigned id&gt;0 when the transaction is committed
 * id=0: Virtual or implicit element that does not physically exist in the database
 * id&gt;0: Physically persisted element
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class AbstractElement implements InternalElement, Comparable<JanusGraphElement> {

    private long id;

    public AbstractElement(long id) {
        this.id = id;
    }

    public static boolean isTemporaryId(long elementId) {
        return elementId < 0;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(getCompareId());
    }

    @Override
    public boolean equals(Object other) {
        if (other==null)
            return false;

        if (this==other)
            return true;
        if (!((this instanceof Vertex && other instanceof Vertex) ||
                (this instanceof Edge && other instanceof Edge) ||
                (this instanceof VertexProperty && other instanceof VertexProperty)))
            return false;
        //Same type => they are the same if they have identical ids.
        if (other instanceof AbstractElement) {
            return getCompareId()==((AbstractElement)other).getCompareId();
        } else if (other instanceof JanusGraphElement) {
            return ((JanusGraphElement) other).hasId() && getCompareId()==((JanusGraphElement)other).longId();
        } else {
            Object otherId = ((Element)other).id();
            if (otherId instanceof RelationIdentifier) return ((RelationIdentifier) otherId).getRelationId()==getCompareId();
            else return otherId.equals(getCompareId());
        }
    }


    @Override
    public int compareTo(JanusGraphElement other) {
        return compare(this,other);
    }

    public static int compare(JanusGraphElement e1, JanusGraphElement e2) {
        long e1id = (e1 instanceof AbstractElement)?((AbstractElement)e1).getCompareId():e1.longId();
        long e2id = (e2 instanceof AbstractElement)?((AbstractElement)e2).getCompareId():e2.longId();
        return Long.compare(e1id,e2id);
    }

    @Override
    public InternalVertex clone() throws CloneNotSupportedException {
        throw new CloneNotSupportedException();
    }

    /* ---------------------------------------------------------------
	 * ID and LifeCycle methods
	 * ---------------------------------------------------------------
	 */

    /**
     * Long identifier used to compare elements. Often, this is the same as {@link #longId()}
     * but some instances of elements may be considered the same even if their ids differ. In that case,
     * this method should be overwritten to return an id that can be used for comparison.
     * @return
     */
    protected long getCompareId() {
        return longId();
    }

    @Override
    public long longId() {
        return id;
    }

    public boolean hasId() {
        return !isTemporaryId(longId());
    }

    @Override
    public void setId(long id) {
        assert id > 0;
        this.id=id;
    }

    @Override
    public boolean isInvisible() {
        return IDManager.VertexIDType.Invisible.is(id);
    }

    @Override
    public boolean isNew() {
        return ElementLifeCycle.isNew(it().getLifeCycle());
    }

    @Override
    public boolean isLoaded() {
        return ElementLifeCycle.isLoaded(it().getLifeCycle());
    }

    @Override
    public boolean isRemoved() {
        return ElementLifeCycle.isRemoved(it().getLifeCycle());
    }

}
