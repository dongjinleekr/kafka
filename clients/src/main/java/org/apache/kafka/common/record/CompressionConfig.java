/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.record;

import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

public class CompressionConfig {
    private final CompressionType type;

    public static CompressionConfig none() {
        return new CompressionConfig(CompressionType.NONE);
    }

    public static CompressionConfig of(final CompressionType type) {
        return new CompressionConfig(Objects.requireNonNull(type));
    }

    private CompressionConfig(final CompressionType type) {
        this.type = type;
    }

    public CompressionType getType() {
        return type;
    }

    /**
     * Wrap bufferStream with an OutputStream that will compress data with this CompressionConfig.
     */
    public OutputStream wrapForOutput(ByteBufferOutputStream bufferStream, byte messageVersion) {
        return type.wrapForOutput(bufferStream, messageVersion, -1);
    }

    /**
     * Wrap buffer with an InputStream that will decompress data with this CompressionConfig.
     */
    public InputStream wrapForInput(ByteBuffer buffer, byte messageVersion, BufferSupplier decompressionBufferSupplier) {
        return type.wrapForInput(buffer, messageVersion, decompressionBufferSupplier);
    }
}
