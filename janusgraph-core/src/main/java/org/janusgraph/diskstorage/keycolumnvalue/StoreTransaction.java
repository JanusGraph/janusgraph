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

package org.janusgraph.diskstorage.keycolumnvalue;

import org.janusgraph.diskstorage.BaseTransactionConfigurable;

/**
 * A transaction handle uniquely identifies a transaction on the storage backend.
 * <p>
 * All modifications to the storage backend must occur within the context of a single
 * transaction. Such a transaction is identified to the JanusGraph middleware by a StoreTransaction.
 * Graph transactions rely on the existence of a storage backend transaction.
 * <p>
 * Note, that a StoreTransaction by itself does not provide any isolation or consistency guarantees (e.g. ACID).
 * Graph Transactions can only extend such guarantees if they are supported by the respective storage backend.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 */
public interface StoreTransaction extends BaseTransactionConfigurable {


}
