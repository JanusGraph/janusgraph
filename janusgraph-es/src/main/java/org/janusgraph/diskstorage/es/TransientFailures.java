// Copyright 2026 JanusGraph Authors
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

package org.janusgraph.diskstorage.es;

import org.apache.http.ConnectionClosedException;
import org.apache.http.HttpEntity;
import org.apache.http.NoHttpResponseException;
import org.apache.http.util.EntityUtils;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.net.ssl.SSLException;

//Recognises the failures which may succeed if the request is reattempted, and the search time-outs which JanusGraph
//doesn't reattempt, whatever their status. Shared by RestElasticSearchClient, which reattempts a single request or the
//failed items of a bulk request a few times, and by ElasticSearchIndex, which classifies whatever survives those
//attempts as temporary or permanent for the longer reattempt loop of BackendOperation. Both act on the same definition
//of transient, so that the two layers cannot disagree
public final class TransientFailures {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private TransientFailures() {
    }

    //Whether the failure occurred before Elasticsearch could produce an HTTP response, and so has no status code to
    //classify on. SocketException covers ConnectException and a connection reset by the peer, while
    //InterruptedIOException covers SocketTimeoutException and ConnectTimeoutException. SSLException is matched whole
    //rather than only the SSLHandshakeException which RestClient rebuilds when a TLS peer goes away mid-request,
    //because a reset during a TLS session surfaces as a plain SSLException and is just as transient; the cost is that
    //a permanently untrusted certificate is reattempted too, which the option description states. SSLException
    //extends IOException directly, so neither of the other two subsumes it
    public static boolean isTransportFailure(Throwable cause) {
        return cause instanceof SocketException
            || cause instanceof InterruptedIOException
            || cause instanceof NoHttpResponseException
            || cause instanceof ConnectionClosedException
            || cause instanceof SSLException;
    }

    //Whether any cause in the chain is a transport failure. The Elasticsearch client rewraps a failure while
    //unwrapping the future it waited on, so the exception which carries the meaning can sit below the one thrown
    public static boolean hasTransportFailureCause(Throwable throwable) {
        for (Throwable cause : causalChain(throwable)) {
            if (isTransportFailure(cause)) {
                return true;
            }
        }
        return false;
    }

    //Whether Elasticsearch failed a search because it ran out of its time, as it does where it was told not to answer
    //with the hits found in time: Elasticsearch 8.15.3 and 9.5.4 fail it with search_timeout_exception, with status 504
    //and 429, and Elasticsearch 7.17 and OpenSearch 2 and 3 with query_phase_execution_exception "Time exceeded", with
    //status 500. Such a search would run as long again on a cluster which was too slow for it already, so neither
    //RestElasticSearchClient nor ElasticSearchIndex reattempts it, whatever its status. The low-level RestClient still
    //sends a request answered with 502, 503 or 504 to its next host, before anything here can tell a time-out
    public static boolean isSearchTimeout(Throwable failure) {
        if (!(failure instanceof ResponseException)) {
            return false;
        }
        final Response response = ((ResponseException) failure).getResponse();
        final HttpEntity entity = response == null ? null : response.getEntity();
        if (entity == null) {
            return false;
        }
        try {
            return mentionsTimeout(MAPPER.readValue(EntityUtils.toString(entity), Object.class));
        } catch (IOException | RuntimeException notJson) {
            return false;
        }
    }

    //The error of a search names the cause of each shard's failure, and the root causes, as objects of a type and a
    //reason
    private static boolean mentionsTimeout(Object node) {
        if (node instanceof Map) {
            final Map<?, ?> error = (Map<?, ?>) node;
            final Object type = error.get("type");
            return "search_timeout_exception".equals(type)
                || "query_phase_execution_exception".equals(type) && "Time exceeded".equals(error.get("reason"))
                || error.values().stream().anyMatch(TransientFailures::mentionsTimeout);
        }
        return node instanceof Collection
            && ((Collection<?>) node).stream().anyMatch(TransientFailures::mentionsTimeout);
    }

    //The throwable followed by its causes, outermost first. A cause chain is not guaranteed to be acyclic - initCause
    //refuses only a self reference, so a loop can be built between two throwables - and the callers walk it inside
    //the catch block of a commit, where spinning would hang the committing thread, so a throwable already seen ends
    //the chain
    public static List<Throwable> causalChain(Throwable throwable) {
        final List<Throwable> chain = new ArrayList<>();
        final Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Throwable cause = throwable; cause != null && visited.add(cause); cause = cause.getCause()) {
            chain.add(cause);
        }
        return chain;
    }
}
