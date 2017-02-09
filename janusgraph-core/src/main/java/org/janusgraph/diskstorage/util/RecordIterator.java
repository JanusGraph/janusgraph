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

package org.janusgraph.diskstorage.util;

import java.io.Closeable;
import java.util.Iterator;

/**
 * Iterator over records in the storage backend. Behaves like a normal iterator
 * with an additional close method so that resources associated with this
 * iterator can be released.
 * 
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface RecordIterator<T> extends Iterator<T>, Closeable { }
