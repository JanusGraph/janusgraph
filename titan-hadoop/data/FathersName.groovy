import com.thinkaurelius.titan.core.TitanFactory
import com.tinkerpop.blueprints.Graph
import org.apache.commons.configuration.BaseConfiguration

Graph g

def setup(args) {
    conf = new BaseConfiguration()
    conf.setProperty('storage.backend', args[0])
    conf.setProperty('storage.hostname', 'localhost')
    g = TitanFactory.open(conf)
}

def map(v, args) {
    u = g.v(v.id) // the Hadoop vertex id is the same as the original Titan vertex id
    pipe = u.out('father').name
    if (pipe.hasNext()) u.fathersName = pipe.next();
    u.name + "'s father's name is " + u.fathersName
}

def cleanup(args) {
    g.shutdown()
}
