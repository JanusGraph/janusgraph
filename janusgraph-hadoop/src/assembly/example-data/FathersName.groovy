// Copyright 2019 JanusGraph Authors
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


import com.tinkerpop.blueprints.Graph
import org.janusgraph.core.JanusGraphFactory
import org.janusgraph.util.system.ConfigurationUtil

Graph g

def setup(args) {
    conf = ConfigurationUtil.createBaseConfiguration()
    conf.setProperty('storage.backend', args[0])
    conf.setProperty('storage.hostname', 'localhost')
    g = JanusGraphFactory.open(conf)
}

def map(v, args) {
    u = g.v(v.id) // the Hadoop vertex id is the same as the original JanusGraph vertex id
    pipe = u.out('father').name
    if (pipe.hasNext()) {
        u.fathersName = pipe.next()
    }
    u.name + "'s father's name is " + u.fathersName
}

def cleanup(args) {
    g.shutdown()
}
