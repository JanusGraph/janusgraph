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

package org.janusgraph.pkgtest;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

public class BerkeleyAssemblyIT extends AbstractJanusGraphAssemblyIT {
    @Override
    protected String getConfigPath() {
        return "conf/janusgraph-berkeleyje.properties";
    }

    @Override
    protected String getServerConfigPath() {
        return "conf/gremlin-server/gremlin-server-berkeleyje.yaml";
    }

    @Override
    protected String getGraphName() {
        return "berkeleyje";
    }

    @AfterEach
    public void cleanupData() throws IOException {
        Path db = Paths.get(BUILD_DIR, "db");
        if (Files.exists(db)){
            try {
                Files.walk(db, FileVisitOption.FOLLOW_LINKS)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .peek(System.out::println)
                    .forEach(File::delete);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    @Disabled
    @Override
    public void testGettingStartedAgainstGremlinSh() {}

    @Test
    @Disabled
    @Override
    public void testGettingStartedAgainstGremlinShFull() {}

    @Test
    @Disabled
    @Override
    public void testSparkGraphComputerTraversalLocal() {
    }
}
