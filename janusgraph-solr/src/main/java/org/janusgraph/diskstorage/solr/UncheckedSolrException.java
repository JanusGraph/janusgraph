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

package org.janusgraph.diskstorage.solr;

import org.apache.solr.client.solrj.SolrServerException;

/**
 * @author davidclement@laposte.net
 */
public class UncheckedSolrException extends RuntimeException {

    private static final long serialVersionUID = -8688401420333013730L;

    /**
     * @param msg   Exception message
     * @param cause Cause of the exception
     */
    public UncheckedSolrException(String msg, SolrServerException cause) {
        super(msg, cause);
    }

    /**
     * Constructs an exception with a generic message
     *
     * @param cause Cause of the exception
     */
    public UncheckedSolrException(SolrServerException cause) {
        this("Permanent failure in Solr backend", cause);
    }
}
