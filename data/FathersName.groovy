def g

def setup(args) {
    conf = new org.apache.commons.configuration.BaseConfiguration()
    conf.setProperty('storage.backend', args[0])
    conf.setProperty('storage.hostname', 'localhost')
    g = com.thinkaurelius.titan.core.TitanFactory.open(conf)
}

def map(v, args) {
    u = g.v(v.id)
    pipe = u.out('father').name
    if (pipe.hasNext()) u.fathersName = pipe.next();
    u.name + "'s father's name is " + u.fathersName
}

def cleanup(args) {
    g.shutdown()
}
