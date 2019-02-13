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

