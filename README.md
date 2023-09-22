[![JanusGraph logo](janusgraph.png)](https://janusgraph.org/)

JanusGraph is a highly scalable [graph database](https://en.wikipedia.org/wiki/Graph_database)
optimized for storing and querying large graphs with billions of vertices and edges
distributed across a multi-machine cluster. JanusGraph is a transactional database that
can support thousands of concurrent users, complex traversals, and analytic graph queries.

[![Downloads][downloads-shield]][downloads-link]
[![Docker pulls][docker-pulls-img]][docker-hub-url]
[![Maven][maven-shield]][maven-link]
[![Javadoc][javadoc-shield]][javadoc-link]
[![GitHub Workflow Status][actions-shield]][actions-link]
[![Codecov][codecov-shield]][codecov-link]
[![Mentioned in Awesome Bigtable][awesome-shield]][awesome-link]
[![CII Best Practices][bestpractices-shield]][bestpractices-link]
[![Codacy Badge][codacy-shield]][codacy-link]

[actions-shield]: https://img.shields.io/github/actions/workflow/status/JanusGraph/janusgraph/ci-core.yml?branch=master
[actions-link]: https://github.com/JanusGraph/janusgraph/actions
[awesome-shield]: https://awesome.re/mentioned-badge-flat.svg
[awesome-link]: https://github.com/zrosenbauer/awesome-bigtable
[bestpractices-shield]: https://bestpractices.coreinfrastructure.org/projects/5064/badge
[bestpractices-link]: https://bestpractices.coreinfrastructure.org/projects/5064
[codacy-shield]: https://app.codacy.com/project/badge/Grade/850c7549ea72424486664ffc4f64f526
[codacy-link]: https://www.codacy.com/gh/JanusGraph/janusgraph/dashboard?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=JanusGraph/janusgraph&amp;utm_campaign=Badge_Grade
[maven-shield]: https://img.shields.io/maven-central/v/org.janusgraph/janusgraph-core.svg
[maven-link]: https://central.sonatype.com/search?q=org.janusgraph
[javadoc-shield]: https://javadoc.io/badge/org.janusgraph/janusgraph-core.svg?color=blue
[javadoc-link]: https://javadoc.io/doc/org.janusgraph/janusgraph-core
[downloads-shield]: https://img.shields.io/github/downloads/JanusGraph/janusgraph/total.svg
[downloads-link]: https://github.com/JanusGraph/janusgraph/releases
[codecov-shield]:https://codecov.io/gh/JanusGraph/janusgraph/branch/master/graph/badge.svg
[codecov-link]:https://codecov.io/gh/JanusGraph/janusgraph
[docker-pulls-img]: https://img.shields.io/docker/pulls/janusgraph/janusgraph.svg
[docker-hub-url]: https://hub.docker.com/r/janusgraph/janusgraph

## Learn More

The [project homepage](https://janusgraph.org) contains more information on JanusGraph and
provides links to documentation, getting-started guides and release downloads.

## Visualization

To visualize graphs stored in JanusGraph, you can use any of the following
tools:

* [Arcade Analytics](https://arcadeanalytics.com/usermanual/#arcade-analytics)
* [Cytoscape](https://www.cytoscape.org/)
* [Gephi](https://tinkerpop.apache.org/docs/current/reference/#gephi-plugin)
  plugin for Apache TinkerPop
* [Graphexp](https://github.com/bricaud/graphexp)
* [Graph Explorer](https://github.com/invanalabs/graph-explorer)
* [Graphlytic](https://graphlytic.com/)
* [Gremlin-Visualizer](https://github.com/prabushitha/gremlin-visualizer)
* [G.V() - Gremlin IDE](https://gdotv.com)
* [KeyLines by Cambridge Intelligence](https://cambridge-intelligence.com/keylines/janusgraph/)
* [Ogma by Linkurious](https://doc.linkurious.com/ogma/latest/tutorials/janusgraph/)
* [ReGraph by Cambridge Intelligence](https://cambridge-intelligence.com/regraph/)
* [Tom Sawyer Perspectives](https://www.tomsawyer.com/perspectives/)

## Community

* GitHub Discussions: see [`GitHub Discussions`](https://github.com/JanusGraph/janusgraph/discussions)
  for all general discussions and questions about JanusGraph

* Discord for interactive discussions and questions about JanusGraph: [Join the server](https://discord.gg/5n4fjv4QAf)

* Stack Overflow: see the
  [`janusgraph`](https://stackoverflow.com/questions/tagged/janusgraph) tag

* Twitter: follow [@JanusGraph](https://twitter.com/JanusGraph) for news and
  updates

* LinkedIn: follow [JanusGraph](https://www.linkedin.com/company/janusgraph) for news and
  updates

* Mailing lists:

  * **janusgraph-users (at) lists.lfaidata.foundation**
    ([archives](https://lists.lfaidata.foundation/g/janusgraph-users/topics))
    for questions about using JanusGraph, installation, configuration, integrations

    To join with a LF AI & Data account, use the [web
    UI](https://lists.lfaidata.foundation/g/janusgraph-users/join); to
    subscribe/unsubscribe with an arbitrary email address, send an email to:

    * janusgraph-users+subscribe (at) lists.lfaidata.foundation
    * janusgraph-users+unsubscribe (at) lists.lfaidata.foundation

  * **janusgraph-dev (at) lists.lfaidata.foundation**
    ([archives](https://lists.lfaidata.foundation/g/janusgraph-dev/topics))
    for internal implementation of JanusGraph itself

    To join with a LF AI & Data account, use the [web
    UI](https://lists.lfaidata.foundation/g/janusgraph-dev/join); to
    subscribe/unsubscribe with an arbitrary email address, send an email to:

    * janusgraph-dev+subscribe (at) lists.lfaidata.foundation
    * janusgraph-dev+unsubscribe (at) lists.lfaidata.foundation

  * **janusgraph-announce (at) lists.lfaidata.foundation**
    ([archives](https://lists.lfaidata.foundation/g/janusgraph-announce/topics))
    for new releases and news announcements

    To join with a LF AI & Data account, use the [web
    UI](https://lists.lfaidata.foundation/g/janusgraph-announce/join); to
    subscribe/unsubscribe with an arbitrary email address, send an email to:

    * janusgraph-announce+subscribe (at) lists.lfaidata.foundation
    * janusgraph-announce+unsubscribe (at) lists.lfaidata.foundation

## Contributing

Please see [`CONTRIBUTING.md`](CONTRIBUTING.md) for more information, including
CLAs and best practices for working with GitHub.

## Powered by JanusGraph

* [Apache Atlas](https://github.com/apache/atlas) - metadata management for governance ([website](https://atlas.apache.org/))
* [Eclipse Keti](https://github.com/eclipse/keti) - access control service to protect RESTful APIs ([website](https://projects.eclipse.org/projects/iot.keti))
* [Exakat](https://github.com/exakat/exakat) - PHP static analysis ([website](https://www.exakat.io/))
* [Open Network Automation Platform (ONAP)](https://www.onap.org/) - automation and orchestration for Software-Defined Networks
- [Uber Knowledge Graph](https://www.youtube.com/watch?v=C01Gh0g01JE) ([event info](https://leap.ai/events/2017/08/06/uber_knowledge_graph))
- [Express-Cassandra](https://github.com/masumsoft/express-cassandra) - Cassandra ORM/ODM/OGM for Node.js with optional support for Elassandra & JanusGraph
* [Windup](https://github.com/windup/windup) by RedHat - application migration and assessment tool ([website](https://developers.redhat.com/products/rhamt/overview/))

## Users

The following users have deployed JanusGraph in production.

* [CELUM](https://www.celum.com/)
* [Crédit Agricole CIB](https://www.ca-cib.com/) - [use case](https://github.com/JanusGraph/janusgraph/discussions/2734)
* [eBay](https://www.ebay.com/) - [video](https://youtu.be/EtB1BPG00PE)
* [FiNC](https://finc.com)
* [G DATA](https://gdatasoftware.com) - [blog post series about malware analysis use case](https://www.gdatasoftware.com/blog/2018/11/31203-malware-analysis-with-a-graph-database)
* [Netflix](https://www.netflix.com) -
  [video](https://youtu.be/KSmAdtMJYEo?t=1h2m17s) and
  [slides](https://www.slideshare.net/RoopaTangirala/polyglot-persistence-netflix-cde-meetup-90955706#86) (graph discussion starts at #86)
* [Qihoo 360](https://www.360.cn/) ([about](https://en.wikipedia.org/wiki/Qihoo_360))
* [Red Hat](https://www.redhat.com/) - [application migration and assessment tool](https://developers.redhat.com/products/rhamt/overview/) built on [Windup](https://github.com/windup/windup)
* [Times Internet](https://timesinternet.in)
* [Uber](https://uber.com)
