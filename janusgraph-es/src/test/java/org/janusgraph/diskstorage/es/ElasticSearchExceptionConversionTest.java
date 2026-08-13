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

import com.google.common.collect.ImmutableSet;
import org.apache.http.ConnectionClosedException;
import org.apache.http.NoHttpResponseException;
import org.apache.http.StatusLine;
import org.apache.http.conn.ConnectTimeoutException;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.util.BackendOperation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

//Tests how ElasticSearchIndex classifies a failed index operation. Only a TemporaryBackendException is reattempted by
//BackendOperation, so a transient failure classified as permanent means the index mutation is dropped and the mixed
//index is left inconsistent with the graph
public class ElasticSearchExceptionConversionTest {

    private static final Set<Integer> DEFAULT_TEMPORARY_ERROR_CODES = ImmutableSet.of(429, 502, 503, 504);

    @AfterEach
    public void clearInterruptStatus() {
        //convert restores the interrupt status of an interrupted failure, which must not leak into the next test
        Thread.interrupted();
    }

    private static BackendException convert(Exception esException) {
        return ElasticSearchIndex.convert(esException, DEFAULT_TEMPORARY_ERROR_CODES, true);
    }

    private static ResponseException responseException(int statusCode) {
        final StatusLine statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(statusCode);
        final Response response = mock(Response.class);
        when(response.getStatusLine()).thenReturn(statusLine);
        final ResponseException responseException = mock(ResponseException.class);
        when(responseException.getResponse()).thenReturn(response);
        return responseException;
    }

    private static ElasticSearchBulkFailureException bulkFailure(Integer... failedItemStatusCodes) {
        return new ElasticSearchBulkFailureException("Failure(s) in Elasticsearch bulk request: [an error]",
            Stream.of(failedItemStatusCodes).collect(ImmutableSet.toImmutableSet()));
    }

    @ParameterizedTest
    @ValueSource(ints = {429, 502, 503, 504})
    public void shouldConvertTransientResponseStatusToTemporary(int statusCode) {
        assertInstanceOf(TemporaryBackendException.class, convert(responseException(statusCode)));
    }

    @ParameterizedTest
    @ValueSource(ints = {400, 401, 403, 404, 409})
    public void shouldConvertPermanentResponseStatusToPermanent(int statusCode) {
        assertInstanceOf(PermanentBackendException.class, convert(responseException(statusCode)));
    }

    @Test
    public void shouldConvertConfiguredResponseStatusToTemporary() {
        final ResponseException requestTimeout = responseException(408);
        assertInstanceOf(PermanentBackendException.class, convert(requestTimeout));
        assertInstanceOf(TemporaryBackendException.class,
            ElasticSearchIndex.convert(requestTimeout, ImmutableSet.of(408), true));
    }

    @Test
    public void shouldConvertResponseStatusToPermanentWhenNoTemporaryCodesConfigured() {
        assertInstanceOf(PermanentBackendException.class,
            ElasticSearchIndex.convert(responseException(503), Collections.emptySet(), true));
    }

    @Test
    public void shouldInspectWholeCauseChain() {
        //RestClient.extractAndWrapCause rewraps the failure of a node attempt, so the exception carrying the status
        //is reached only through the cause chain
        assertInstanceOf(TemporaryBackendException.class, convert(new IOException(
            "method [POST], host [http://localhost:9200], status line [HTTP/1.1 503 Service Unavailable]",
            responseException(503))));
    }

    @Test
    public void shouldConvertBulkFailureOfEntirelyTransientItemsToTemporary() {
        assertInstanceOf(TemporaryBackendException.class, convert(bulkFailure(429, 503)));
    }

    @Test
    public void shouldConvertPartiallyPermanentBulkFailureToPermanent() {
        //A reattempt of the whole batch cannot succeed while one of its items fails permanently
        assertInstanceOf(PermanentBackendException.class, convert(bulkFailure(429, 400)));
    }

    @Test
    public void shouldConvertBulkFailureWithoutStatusCodesToPermanent() {
        assertInstanceOf(PermanentBackendException.class, convert(bulkFailure()));
    }

    @Test
    public void shouldConvertTransportFailuresToTemporary() {
        //None of these produce an HTTP response, so no status code is available to classify on
        assertInstanceOf(TemporaryBackendException.class, convert(new ConnectException("Connection refused")));
        assertInstanceOf(TemporaryBackendException.class, convert(new SocketException("Connection reset")));
        assertInstanceOf(TemporaryBackendException.class, convert(new SocketTimeoutException("30,000 milliseconds timeout on connection")));
        assertInstanceOf(TemporaryBackendException.class, convert(new ConnectTimeoutException("connect timed out")));
        assertInstanceOf(TemporaryBackendException.class, convert(new NoHttpResponseException("failed to respond")));
        assertInstanceOf(TemporaryBackendException.class, convert(new ConnectionClosedException("Connection closed")));
    }

    @Test
    public void shouldConvertTransportFailuresToPermanentWhenDisabled() {
        assertInstanceOf(PermanentBackendException.class,
            ElasticSearchIndex.convert(new SocketTimeoutException("read timed out"),
                DEFAULT_TEMPORARY_ERROR_CODES, false));
        //Disabling transport failure classification must not affect classification by status code
        assertInstanceOf(TemporaryBackendException.class,
            ElasticSearchIndex.convert(responseException(503), DEFAULT_TEMPORARY_ERROR_CODES, false));
    }

    @Test
    public void shouldConvertInterruptToTemporary() {
        final InterruptedException interrupted = new InterruptedException();
        BackendException converted = convert(interrupted);
        assertInstanceOf(TemporaryBackendException.class, converted);
        assertEquals("Interrupted while waiting for response", converted.getMessage());
        assertSame(interrupted, converted.getCause());

        //An interrupt during a client side retry wait reaches convert wrapped in a RuntimeException
        converted = convert(new RuntimeException("Thread interrupted while waiting for retry attempt 1 of 3",
            interrupted));
        assertInstanceOf(TemporaryBackendException.class, converted);
        assertEquals("Interrupted while waiting for response", converted.getMessage());
    }

    @Test
    public void shouldRestoreInterruptStatusOfInterruptedFailure() {
        //Throwing the InterruptedException cleared the status, and convert consumes the exception itself, so the
        //status is the only remaining signal that the operation was cancelled
        assertFalse(Thread.currentThread().isInterrupted());
        convert(new RuntimeException("Thread interrupted while waiting for retry attempt 1 of 3",
            new InterruptedException()));
        assertTrue(Thread.currentThread().isInterrupted());
    }

    @Test
    public void shouldNotReattemptInterruptedFailure() {
        //BackendOperation aborts its backoff wait only while the interrupt status is set. Without it a cancelled
        //commit would keep reissuing the bulk request for the whole write time budget
        final AtomicInteger attempts = new AtomicInteger();
        assertThrows(PermanentBackendException.class, () -> BackendOperation.executeDirect(() -> {
            attempts.incrementAndGet();
            throw convert(new RuntimeException("Thread interrupted while waiting for retry attempt 1 of 3",
                new InterruptedException()));
        }, Duration.ofSeconds(30)));
        assertEquals(1, attempts.get());
    }

    @Test
    public void shouldConvertUnrecognisedFailureToPermanent() {
        final IllegalArgumentException tooLarge = new IllegalArgumentException(
            "Bulk request item(s) larger than permitted chunk limit.");
        final BackendException converted = convert(tooLarge);
        assertInstanceOf(PermanentBackendException.class, converted);
        assertSame(tooLarge, converted.getCause());
    }

    @Test
    public void shouldRetainCauseOfTemporaryFailure() {
        final ResponseException responseException = responseException(503);
        final IOException wrapper = new IOException(responseException);
        assertSame(wrapper, convert(wrapper).getCause());
    }

    @Test
    public void shouldReattemptTransientFailureUntilItSucceeds() throws BackendException {
        //IndexTransaction submits the mutation through BackendOperation, which reattempts it only while it fails
        //temporarily
        final AtomicInteger attempts = new AtomicInteger();
        final boolean mutated = BackendOperation.executeDirect(() -> {
            if (attempts.incrementAndGet() < 3) {
                throw convert(responseException(503));
            }
            return true;
        }, Duration.ofSeconds(30));
        assertTrue(mutated);
        assertEquals(3, attempts.get());
    }

    @Test
    public void shouldNotReattemptPermanentFailure() {
        final AtomicInteger attempts = new AtomicInteger();
        assertThrows(PermanentBackendException.class, () -> BackendOperation.executeDirect(() -> {
            attempts.incrementAndGet();
            throw convert(responseException(400));
        }, Duration.ofSeconds(30)));
        assertEquals(1, attempts.get());
    }
}
