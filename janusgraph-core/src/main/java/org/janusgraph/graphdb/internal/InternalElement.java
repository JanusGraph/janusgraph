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

import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;

/**
 * Internal Element interface adding methods that should only be used by JanusGraph
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface InternalElement extends JanusGraphElement {

    /**
     * Returns this element in the context of the current transaction.
     *
     * @return
     */
    InternalElement it();

    /**
     * Returns the transaction to which the element is currently bound or should be refreshed into
     * @return
     */
    StandardJanusGraphTx tx();


    @Override
    default JanusGraphTransaction graph() {
        return tx();
    }

    void setId(long id);

    /**
     * @see ElementLifeCycle
     * @return The lifecycle of this element
     */
    byte getLifeCycle();

    /**
     * Whether this element is invisible and should only be returned to queries that explicitly ask for invisible elements.
     * @return
     */
    boolean isInvisible();

}
