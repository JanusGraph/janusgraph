/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * This file was pulled from the Apache TinkerPop project:
 * https://github.com/apache/tinkerpop/blob/3.3.3/gremlin-server/src/test/scripts/test-server-start.groovy
 * and has been stripped down to the basic functionality needed here.
 */

import org.apache.tinkerpop.gremlin.server.GremlinServer
import org.apache.tinkerpop.gremlin.server.Settings

if (Boolean.parseBoolean(skipTests)) return

log.info("Starting Gremlin Server instances for native testing of ${executionName}")
def settings = Settings.read("${settingsFile}")
settings.graphs.gods = janusgraphServerDir + "/src/test/scripts/janusgraph-server-empty.properties"
settings.scriptEngines["gremlin-groovy"].plugins["org.apache.tinkerpop.gremlin.jsr223.ScriptFileGremlinPlugin"].files = [janusgraphServerDir + "/src/test/scripts/generate-graphs.groovy"]

def server = new GremlinServer(settings)
server.start().join()

project.setContextValue("janusgraph.server", server)
log.info("Gremlin Server with no authentication started")