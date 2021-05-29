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

package org.janusgraph.core.schema;

import org.easymock.EasyMockSupport;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.attribute.Geoshape;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.UUID;

import static org.easymock.EasyMock.expect;

public class JanusGraphDefaultSchemaMakerTest extends EasyMockSupport {

  @Test
  public void testMakePropertyKey() {
      PropertyKeyMaker pkm = mockPropertyKeyMaker();
      DefaultSchemaMaker schemaMaker = JanusGraphDefaultSchemaMaker.INSTANCE;
      byte b = 100;
      short s = 10000;
      schemaMaker.makePropertyKey(pkm, "Foo");
      schemaMaker.makePropertyKey(pkm, 'f');
      schemaMaker.makePropertyKey(pkm, true);
      schemaMaker.makePropertyKey(pkm, b);
      schemaMaker.makePropertyKey(pkm, s);
      schemaMaker.makePropertyKey(pkm, 100);
      schemaMaker.makePropertyKey(pkm, 100L);
      schemaMaker.makePropertyKey(pkm, 100.0f);
      schemaMaker.makePropertyKey(pkm, 1.23e2);
      schemaMaker.makePropertyKey(pkm, new Date());
      schemaMaker.makePropertyKey(pkm, Geoshape.point(42.3601f, 71.0589f));
      schemaMaker.makePropertyKey(pkm, UUID.randomUUID());
      schemaMaker.makePropertyKey(pkm, new Object());

      verifyAll();
  }

  private PropertyKeyMaker mockPropertyKeyMaker() {
      PropertyKeyMaker propertyKeyMaker = createMock(PropertyKeyMaker.class);
      PropertyKey pk = createMock(PropertyKey.class);
      expect(propertyKeyMaker.make()).andReturn(pk).anyTimes();
      expect(propertyKeyMaker.getName()).andReturn("Quux").anyTimes();
      expect(propertyKeyMaker.cardinality(Cardinality.SINGLE)).andReturn(propertyKeyMaker).anyTimes();
      expect(propertyKeyMaker.cardinalityIsSet()).andReturn(false).anyTimes();
      expect(propertyKeyMaker.dataType(String.class)).andReturn(propertyKeyMaker);
      expect(propertyKeyMaker.dataType(Character.class)).andReturn(propertyKeyMaker);
      expect(propertyKeyMaker.dataType(Boolean.class)).andReturn(propertyKeyMaker);
      expect(propertyKeyMaker.dataType(Byte.class)).andReturn(propertyKeyMaker);
      expect(propertyKeyMaker.dataType(Short.class)).andReturn(propertyKeyMaker);
      expect(propertyKeyMaker.dataType(Integer.class)).andReturn(propertyKeyMaker);
      expect(propertyKeyMaker.dataType(Long.class)).andReturn(propertyKeyMaker);
      expect(propertyKeyMaker.dataType(Float.class)).andReturn(propertyKeyMaker);
      expect(propertyKeyMaker.dataType(Double.class)).andReturn(propertyKeyMaker);
      expect(propertyKeyMaker.dataType(Date.class)).andReturn(propertyKeyMaker);
      expect(propertyKeyMaker.dataType(Geoshape.class)).andReturn(propertyKeyMaker);
      expect(propertyKeyMaker.dataType(UUID.class)).andReturn(propertyKeyMaker);
      expect(propertyKeyMaker.dataType(Object.class)).andReturn(propertyKeyMaker);
      replayAll();
      return propertyKeyMaker;
  }

}
