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

import org.apache.kafka.common.utils.ByteBufferOutputStream;

import java.io.DataOutputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * This class holds all compression configurations: compression type, compression level and the size of compression buffer.
 */
public class CompressionConfig {
    private final CompressionType type;
    private final Integer level;
    private final Integer bufferSize;

    private CompressionConfig(CompressionType type, Integer level, Integer bufferSize) {
        this.type = type;
        this.level = level;
        this.bufferSize = bufferSize;
    }

    public CompressionType getType() {
        return type;
    }

    public Integer getLevel() {
        return level;
    }

    public Integer getBufferSize() {
        return bufferSize;
    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + (level != null ? level.hashCode() : 0);
        result = 31 * result + (bufferSize != null ? bufferSize.hashCode() : 0);
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CompressionConfig that = (CompressionConfig) o;

        if (type != that.type) return false;
        if (level != null ? !level.equals(that.level) : that.level != null) return false;
        return bufferSize != null ? bufferSize.equals(that.bufferSize) : that.bufferSize == null;
    }

    /**
     * Returns an {@link DataOutputStream} that compresses given bytes into <code>output</code> {@link ByteBufferOutputStream}
     * with specified <code>magic</code>.
     */
    public DataOutputStream outputStream(ByteBufferOutputStream output, byte magic) {
        return new DataOutputStream(type.wrapForOutput(output, magic, this.level, this.bufferSize));
    }

    /**
     * Creates a not-compressing configuration.
     */
    public static CompressionConfig none() {
        return of(CompressionType.NONE);
    }

    /**
     * Creates a configuration of specified {@link CompressionType}, default compression level, and compression buffer size.
     */
    public static CompressionConfig of(CompressionType type) {
        return of(type, null);
    }

    /**
     * Creates a configuration of specified {@link CompressionType}, specified compression level, and default compression buffer size.
     */
    public static CompressionConfig of(CompressionType type, Integer level) {
        return of(type, level, null);
    }

    /**
     * Creates a configuration of specified {@link CompressionType}, compression level, and compression buffer size.
     */
    public static CompressionConfig of(CompressionType type, Integer level, Integer bufferSize) {
        return new CompressionConfig(type, level, bufferSize);
    }

    /**
     * Creates a map from {@link CompressionType} to {@link CompressionConfig} from a comma separated string.
     */
    public static Map<CompressionType, CompressionConfig> from(String str) {
        Map<String, String> configMap = new HashMap<>();

        for (String entity : str.split(",")) {
            String[] tokens = entity.split(":");

            if (tokens.length == 2) {
                configMap.put(tokens[0], tokens[1]);
            }
        }

        Map<CompressionType, CompressionConfig> ret = new HashMap<>();

        for (CompressionType compressionType : CompressionType.values()) {
            Integer level = null;
            Integer bufferSize = null;

            try {
                level = Integer.parseInt(configMap.get(compressionType.name + ".level"));
                bufferSize = Integer.parseInt(configMap.get(compressionType.name + ".buffer.size"));
            } catch (NumberFormatException e) {
                // ignore the exception
            } finally {
                ret.put(compressionType, CompressionConfig.of(compressionType, level, bufferSize));
            }
        }

        return ret;
    }
}
