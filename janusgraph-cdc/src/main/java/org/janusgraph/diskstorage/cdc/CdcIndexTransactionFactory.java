// Copyright 2025 JanusGraph Authors
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

package org.janusgraph.diskstorage.cdc;

import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.indexing.IndexTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating CDC-enabled index transactions.
 * Manages the lifecycle of CDC producers and provides wrapped index transactions.
 */
public class CdcIndexTransactionFactory implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(CdcIndexTransactionFactory.class);

    private final CdcConfiguration cdcConfig;
    private final CdcProducer cdcProducer;

    public CdcIndexTransactionFactory(Configuration configuration) {
        this.cdcConfig = new CdcConfiguration(configuration);
        
        if (cdcConfig.isEnabled()) {
            this.cdcProducer = new KafkaCdcProducer(
                cdcConfig.getKafkaBootstrapServers(),
                cdcConfig.getKafkaTopic()
            );
            log.info("CDC Index Transaction Factory initialized");
        } else {
            this.cdcProducer = null;
        }
    }

    /**
     * Wrap an IndexTransaction with CDC support if enabled.
     *
     * @param indexTransaction The base index transaction to wrap
     * @return The wrapped transaction if CDC is enabled, otherwise null (caller should use original)
     */
    public CdcIndexTransaction wrapIfEnabled(IndexTransaction indexTransaction) {
        if (cdcConfig.isEnabled() && cdcProducer != null) {
            return new CdcIndexTransaction(indexTransaction, cdcProducer, cdcConfig.getMode());
        }
        return null;
    }

    /**
     * Check if CDC is enabled.
     *
     * @return true if CDC is enabled
     */
    public boolean isEnabled() {
        return cdcConfig.isEnabled();
    }

    @Override
    public void close() {
        if (cdcProducer != null) {
            cdcProducer.close();
            log.info("CDC Index Transaction Factory closed");
        }
    }
}
