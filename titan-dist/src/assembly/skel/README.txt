-= Titan: Distributed Graph Database =-

Titan is a distributed graph database optimized for storing
and processing large-scale graphs within a multi-machine cluster.
The primary features of Titan are itemized below.

  Support for various distributed storage layers
    * BerkeleyDB
    * Cassandra
    * HBase

  Natively implements the Blueprints graph API
    * Gremlin graph traversal language
    * Frames graph-to-object mapper
    * Rexster graph server

  Open source with the liberal Apache 2 license

Titan documentation can be found online at:
            https://github.com/thinkaurelius/titan/wiki

----------------- RUNNING TITAN -------------------------

1. Running Titan/BerkeleyDB via Gremlin

titan$ bin/gremlin.sh

         \,,,/
         (o o)
-----oOOo-(_)-oOOo-----
gremlin> g = TitanFactory.open('/tmp/titan-local');
==>titangraph[local:/tmp/titan-local]


2. Running Titan/Cassandra via Gremlin

titan$ cassandra
titan$ bin/gremlin.sh

         \,,,/
         (o o)
-----oOOo-(_)-oOOo-----
gremlin> conf = new BaseConfiguration();
gremlin> conf.setProperty("storage.backend","cassandra");
gremlin> conf.setProperty("storage.hostname","127.0.0.1");
gremlin> g = TitanFactory.open(conf);
==>titangraph[cassandra:127.0.0.1]

It is possible to use TitanFactory.open('bin/cassandra.local') to reduce verbosity of opening a local instance.

3. Running Titan/HBase via Gremlin

titan$ start-hbase.sh
titan$ bin/gremlin.sh

         \,,,/
         (o o)
-----oOOo-(_)-oOOo-----
gremlin> conf = new BaseConfiguration();
gremlin> conf.setProperty("storage.backend","hbase");
gremlin> conf.setProperty("storage.hostname","127.0.0.1");
gremlin> g = TitanFactory.open(conf);
==>titangraph[hbase:127.0.0.1]

It is possible to use TitanFactory.open('bin/hbase.local') to reduce verbosity of opening a local instance.

----------------------------------------------------------

Titan is provided by Aurelius [http://thinkaurelius.com]
    "Applying Network Science and Graph Theory"
