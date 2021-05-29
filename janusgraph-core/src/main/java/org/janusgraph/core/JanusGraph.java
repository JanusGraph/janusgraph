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

package org.janusgraph.core;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.util.Gremlin;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.configuration.JanusGraphConstants;

/**
 * JanusGraph graph database implementation of the Blueprint's interface.
 * Use {@link JanusGraphFactory} to open and configure JanusGraph instances.
 *
 * @author Matthias Br&ouml;cheler (http://www.matthiasb.com)
 * @see JanusGraphFactory
 * @see JanusGraphTransaction
 */
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_COMPUTER)
//------------------------
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.VertexPropertyTest$VertexPropertyAddition",
        method = "shouldHandleSetVertexProperties",
        reason = "JanusGraph can only handle SET cardinality for properties when defined in the schema.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.VertexPropertyTest$VertexPropertyAddition",
        method = "shouldHandleListVertexPropertiesWithoutNullPropertyValues",
        reason = "This test case requires EmptyVertexProperty instance when setting null value to a property, while JanusGraph " +
            "returns an EmptyJanusGraphVertexProperty instance in such case.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexTest",
        method = "g_V_hasLabelXpersonX_propertyXname_nullX",
        reason = "TinkerPop assumes cardinality is SINGLE when not explicitly given, while JanusGraph uses the cardinality " +
            "already defined in the schema.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeTest",
        method = "g_V_outE_propertyXweight_nullX",
        reason = "TinkerPop assumes cardinality is SINGLE when not explicitly given, while JanusGraph uses the cardinality " +
            "already defined in the schema.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.computer.GraphComputerTest",
        method = "shouldOnlyAllowReadingVertexPropertiesInMapReduce",
        reason = "JanusGraph simply throws the wrong exception -- should not be a ReadOnly transaction exception but a specific one for MapReduce. This is too cumbersome to refactor in JanusGraph.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.computer.GraphComputerTest",
        method = "shouldProcessResultGraphNewWithPersistVertexProperties",
        reason = "The result graph should return an empty iterator when vertex.edges() or vertex.vertices() is called.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.io.IoTest$GraphMLTest",
        method = "shouldReadGraphMLWithNoEdgeLabels",
        reason = "JanusGraph does not support default edge label (edge) used when GraphML is missing edge labels.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.io.IoTest$GraphMLTest",
        method = "shouldReadGraphMLWithoutEdgeIds",
        reason = "JanusGraph does not support default edge label (edge) used when GraphML is missing edge ids.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.computer.GraphComputerTest",
        method = "shouldSupportGraphFilter",
        reason = "JanusGraph test graph computer (FulgoraGraphComputer) " +
            "currently does not support graph filters but does not throw proper exception because doing so breaks numerous " +
            "tests in gremlin-test ProcessComputerSuite.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategyProcessTest",
        method = "shouldResetAfterRollback",
        reason = "JanusGraph assumes lifecycle of transactionListeners in AbstractThreadLocalTransaction ends when the " +
            "transaction ends (commit/rollback/close). TinkerPop, however, asserts transactionListeners are active across transactions.")
public interface JanusGraph extends Transaction {

   /* ---------------------------------------------------------------
    * Transactions and general admin
    * ---------------------------------------------------------------
    */

    /**
     * Opens a new thread-independent {@link JanusGraphTransaction}.
     * <p>
     * The transaction is open when it is returned but MUST be explicitly closed by calling {@link org.janusgraph.core.JanusGraphTransaction#commit()}
     * or {@link org.janusgraph.core.JanusGraphTransaction#rollback()} when it is no longer needed.
     * <p>
     * Note, that this returns a thread independent transaction object. It is not necessary to call this method
     * to use Blueprint's standard transaction framework which will automatically start a transaction with the first
     * operation on the graph.
     *
     * @return Transaction object representing a transactional context.
     */
    JanusGraphTransaction newTransaction();

    /**
     * Returns a {@link TransactionBuilder} to construct a new thread-independent {@link JanusGraphTransaction}.
     *
     * @return a new TransactionBuilder
     * @see TransactionBuilder
     * @see #newTransaction()
     */
    TransactionBuilder buildTransaction();

    /**
     * Returns the management system for this graph instance. The management system provides functionality
     * to change global configuration options, install indexes and inspect the graph schema.
     * <p>
     * The management system operates in its own transactional context which must be explicitly closed.
     *
     * @return
     */
    JanusGraphManagement openManagement();

    /**
     * Checks whether the graph is open.
     *
     * @return true, if the graph is open, else false.
     * @see #close()
     */
    boolean isOpen();

    /**
     * Checks whether the graph is closed.
     *
     * @return true, if the graph has been closed, else false
     */
    boolean isClosed();

    /**
     * Closes the graph database.
     * <p>
     * Closing the graph database causes a disconnect and possible closing of the underlying storage backend
     * and a release of all occupied resources by this graph database.
     * Closing a graph database requires that all open thread-independent transactions have been closed -
     * otherwise they will be left abandoned.
     *
     * @throws JanusGraphException if closing the graph database caused errors in the storage backend
     */
    @Override
    void close() throws JanusGraphException;

    /**
     * The version of this JanusGraph graph database
     *
     * @return
     */
    static String version() {
        return JanusGraphConstants.VERSION;
    }

    static void main(String[] args) {
        System.out.println("JanusGraph " + JanusGraph.version() + ", Apache TinkerPop " + Gremlin.version());
    }
}
