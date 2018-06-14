CREATE TABLE {{ cassandra_keyspace }}.txlog (
    key blob,
    column1 blob,
    value blob,
    PRIMARY KEY (key, column1)
) WITH COMPACT STORAGE
    AND speculative_retry = 'NONE';

CREATE TABLE {{ cassandra_keyspace }}.edgestore (
    key blob,
    column1 blob,
    value blob,
    PRIMARY KEY (key, column1)
) WITH COMPACT STORAGE
    AND speculative_retry = 'NONE';

CREATE TABLE {{ cassandra_keyspace }}.edgestore_lock_ (
    key blob,
    column1 blob,
    value blob,
    PRIMARY KEY (key, column1)
) WITH COMPACT STORAGE
    AND speculative_retry = 'NONE';

CREATE TABLE {{ cassandra_keyspace }}.systemlog (
    key blob,
    column1 blob,
    value blob,
    PRIMARY KEY (key, column1)
) WITH COMPACT STORAGE
    AND speculative_retry = 'NONE';

CREATE TABLE {{ cassandra_keyspace }}.janusgraph_ids (
    key blob,
    column1 blob,
    value blob,
    PRIMARY KEY (key, column1)
) WITH COMPACT STORAGE
    AND speculative_retry = 'NONE';

CREATE TABLE {{ cassandra_keyspace }}.graphindex (
    key blob,
    column1 blob,
    value blob,
    PRIMARY KEY (key, column1)
) WITH COMPACT STORAGE
    AND speculative_retry = 'NONE';

CREATE TABLE {{ cassandra_keyspace }}.graphindex_lock_ (
    key blob,
    column1 blob,
    value blob,
    PRIMARY KEY (key, column1)
) WITH COMPACT STORAGE
    AND speculative_retry = 'NONE';

CREATE TABLE {{ cassandra_keyspace }}.system_properties (
    key blob,
    column1 blob,
    value blob,
    PRIMARY KEY (key, column1)
) WITH COMPACT STORAGE
    AND speculative_retry = 'NONE';

CREATE TABLE {{ cassandra_keyspace }}.system_properties_lock_ (
    key blob,
    column1 blob,
    value blob,
    PRIMARY KEY (key, column1)
) WITH COMPACT STORAGE
    AND speculative_retry = 'NONE';

