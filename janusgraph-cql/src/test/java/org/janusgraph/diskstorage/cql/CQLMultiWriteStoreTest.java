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

package org.janusgraph.diskstorage.cql;

import com.datastax.oss.driver.internal.core.session.DefaultSession;
import org.apache.commons.lang.UnhandledException;
import org.apache.logging.log4j.Level;
import org.janusgraph.JanusGraphCassandraContainer;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.MultiWriteKeyColumnValueStoreTest;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.testutil.TestLoggerUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.List;

import static org.janusgraph.diskstorage.cql.CQLConfigOptions.BATCH_STATEMENT_SIZE;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.SESSION_LEAK_THRESHOLD;

@Testcontainers
public class CQLMultiWriteStoreTest extends MultiWriteKeyColumnValueStoreTest {

    @Container
    public static final JanusGraphCassandraContainer cqlContainer = new JanusGraphCassandraContainer();

    protected ModifiableConfiguration getBaseStorageConfiguration() {
        return cqlContainer.getConfiguration(getClass().getSimpleName());
    }

    private CQLStoreManager openStorageManager(final Configuration c) throws BackendException {
        return new CachingCQLStoreManager(c);
    }

    @Override
    public CQLStoreManager openStorageManager() throws BackendException {
        return openStorageManager(getBaseStorageConfiguration());
    }

    @Test
    public void shouldLogSessionLeakWarning() throws BackendException {

        TestLoggerUtils.processWithLogCapture(DefaultSession.class, Level.WARN, capture -> {

            ModifiableConfiguration configuration = getBaseStorageConfiguration();
            configuration.set(SESSION_LEAK_THRESHOLD, 2);

            Assertions.assertFalse(hasWarnLog(capture));

            List<CQLStoreManager> storeManagers = new ArrayList<>(3);
            for(int i=0; i<3; i++){
                try {
                    storeManagers.add(new CQLStoreManager(configuration));
                } catch (BackendException e) {
                    Assertions.fail();
                }
            }

            Assertions.assertTrue(hasWarnLog(capture));

            storeManagers.forEach(cqlStoreManager -> {
                try{
                    cqlStoreManager.close();
                } catch (BackendException backendException){
                    throw new UnhandledException(backendException);
                }
            });

        });
    }

    @Test
    public void shouldProperlyCloseSessionOnExceptionAndNotLogSessionLeakWarnings() {

        TestLoggerUtils.processWithLogCapture(DefaultSession.class, Level.WARN, capture -> {

            ModifiableConfiguration configuration = Mockito.spy(getBaseStorageConfiguration());
            configuration.set(SESSION_LEAK_THRESHOLD, 2);

            Mockito.doThrow(RuntimeException.class).when(configuration).get(BATCH_STATEMENT_SIZE);

            Assertions.assertFalse(hasWarnLog(capture));

            for(int i=0; i<3; i++){
                Assertions.assertThrows(Throwable.class, () -> new CQLStoreManager(configuration));
            }

            Assertions.assertFalse(hasWarnLog(capture));

        });
    }

    private boolean hasWarnLog(TestLoggerUtils.LogCapture capture){
        return capture.getEvents().stream().anyMatch(logEvent -> Level.WARN.equals(logEvent.getLevel()) &&
            logEvent.getMessage().getFormattedMessage().startsWith("You have too many session instances"));
    }
}
