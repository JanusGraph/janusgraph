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

package org.janusgraph.util.datastructures;

import java.util.Collection;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface Interval<T> {

    public T getStart();

    public T getEnd();

    public boolean startInclusive();

    public boolean endInclusive();

    public boolean isPoints();

    public Collection<T> getPoints();

    public boolean isEmpty();

    public Interval<T> intersect(Interval<T> other);

}
