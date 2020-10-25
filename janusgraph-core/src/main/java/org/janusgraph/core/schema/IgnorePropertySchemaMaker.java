// Copyright 2020 JanusGraph Authors
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

package org.janusgraph.core.schema;

import org.janusgraph.core.PropertyKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IgnorePropertySchemaMaker extends DisableDefaultSchemaMaker {

    public static final IgnorePropertySchemaMaker INSTANCE = new IgnorePropertySchemaMaker();
    private static final Logger log = LoggerFactory.getLogger(IgnorePropertySchemaMaker.class);

    private boolean loggingEnabled;

    @Override
    public void enableLogging(Boolean enabled) {
        if (Boolean.TRUE.equals(enabled)) {
            loggingEnabled = true;
        }
    }

    @Override
    public PropertyKey makePropertyKey(PropertyKeyMaker factory) {
        if (loggingEnabled) {
            log.warn("Property key '{}' does not exist, will ignore", factory.getName());
        }
        return null;
    }
}
