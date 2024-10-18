// Copyright 2024 JanusGraph Authors
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

package org.janusgraph.core.schema.json.parser;

import org.apache.commons.lang.UnhandledException;
import org.janusgraph.core.schema.Parameter;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class EnumJsonParameterParser implements JsonParameterParser {

    @Override
    public Parameter parse(String key, String value) {
        return Parameter.of(key, toEnum(value));
    }

    private Object toEnum(String value){

        int indexOfValue = value.lastIndexOf(".");

        if(indexOfValue == -1){
            throw new AssertionError(value+" isn't a valid enum value. Please use full path (package + class + value) to enum value. " +
                "Example: org.janusgraph.core.schema.Mapping.TEXT");
        }

        Class enumClass;
        Method method;

        try {
            enumClass = Class.forName(value.substring(0,indexOfValue));
            method = enumClass.getMethod("valueOf", String.class);
            return method.invoke(null, value.substring(indexOfValue+1));
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new UnhandledException(e);
        }
    }
}
