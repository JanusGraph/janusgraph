package com.thinkaurelius.titan.graphdb.transaction;

import com.thinkaurelius.titan.core.Node;
import com.thinkaurelius.titan.graphdb.vertices.NewEmptyNode;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class InMemoryIndex {


    public static final Node NO_INDEX_ENTRY = new NewEmptyNode() {
        @Override
        public GraphTx getTransaction() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void delete() {
            throw new UnsupportedOperationException();
        }
    };

}
