/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.function.Function;

public class ValueTransformerFactory {
    public static <T> Function<T, T> createTransformer(RuntimeContext context) {
        if (context.getExecutionConfig().isObjectReuseEnabled()) {
            return new CopyValueTransformer<>();
        }
        return Function.identity();
    }

    private static class CopyValueTransformer<T> implements Function<T, T> {
        private TypeSerializer<T> serializer;

        @Override
        public T apply(T t) {
            if (serializer == null) {
                initializeSerializer(t);
            }

            return serializer.copy(t);
        }

        private void initializeSerializer(T t) {
            TypeInformation<?> typeInformation = TypeInformation.of(t.getClass());
            ExecutionConfig executionConfig = ExecutionEnvironment
                    .getExecutionEnvironment()
                    .getConfig();
            serializer = (TypeSerializer<T>) typeInformation.createSerializer(executionConfig);
        }
    }
}
