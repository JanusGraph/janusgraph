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

package org.janusgraph.core.util;

import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;

import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;

public class JsonUtil {

    public static <T> T jsonResourcePathToObject(String resourcePath, Class<T> parsedClass) throws IOException {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        InputStream resourceStream = loader.getResourceAsStream(resourcePath);
        return jsonToObject(new InputStreamReader(resourceStream), parsedClass);
    }

    public static <T> T jsonFilePathToObject(String filePath, Class<T> parsedClass) throws IOException {
        return jsonToObject(new FileReader(filePath), parsedClass);
    }

    public static <T> T jsonStringToObject(String json, Class<T> parsedClass) throws IOException {
        return jsonToObject(new StringReader(json), parsedClass);
    }

    public static <T> T jsonToObject(Reader reader, Class<T> parsedClass) throws IOException {
        return new ObjectMapper().readValue(reader, parsedClass);
    }

}
