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
package org.apache.kafka.streams.integration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.Is.is;

@Category( {IntegrationTest.class})
public class KStreamMergeTest {
    public static class TwoPartitioner implements Partitioner {
        @Override
        public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
            return (((Long) key).intValue() - 1) % 2;
        }

        @Override
        public void close() {

        }

        @Override
        public void configure(Map<String, ?> configs) {

        }
    }

    public static class FourPartitioner implements Partitioner {
        @Override
        public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
            return (((Long) key).intValue() - 1) % 4;
        }

        @Override
        public void close() {

        }

        @Override
        public void configure(Map<String, ?> configs) {

        }
    }

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

    private static final String INPUT_TOPIC_RIGHT = "inputTopicRight";
    private static final String INPUT_TOPIC_LEFT = "inputTopicLeft";
    private static final String OUTPUT_TOPIC = "outputTopic";

    @Before
    public void setUp() throws InterruptedException {
        CLUSTER.createTopic(INPUT_TOPIC_LEFT, 2, 1);
        CLUSTER.createTopic(INPUT_TOPIC_RIGHT, 4, 1);
        CLUSTER.createTopic(OUTPUT_TOPIC, 2, 1);
    }

    @After
    public void cleanUp() throws InterruptedException {
        CLUSTER.deleteAllTopicsAndWait(120000);
    }

    private Properties producerConfig() {
        Properties ret = new Properties();

        ret.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        ret.put(ProducerConfig.ACKS_CONFIG, "all");
        ret.put(ProducerConfig.RETRIES_CONFIG, 0);
        ret.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        ret.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        return ret;
    }

    private Properties streamsConfig() {
        Properties ret = new Properties();

        ret.put(StreamsConfig.APPLICATION_ID_CONFIG, "kstream-merge-test");
        ret.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        ret.put(IntegrationTestUtils.INTERNAL_LEAVE_GROUP_ON_CLOSE, true);
        ret.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        ret.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        ret.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        ret.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);

        return ret;
    }

    private Properties consumerConfig() {
        Properties ret = new Properties();

        ret.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        ret.put(ConsumerConfig.GROUP_ID_CONFIG, "kstream-merge-test-result");
        ret.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        ret.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        ret.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        return ret;
    }

    private Topology topology() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<Long, String> leftStream = builder.stream(INPUT_TOPIC_LEFT);
        KStream<Long, String> rightStream = builder.stream(INPUT_TOPIC_RIGHT);
        KStream<Long, String> outputStream = leftStream.merge(rightStream);
        outputStream.to(OUTPUT_TOPIC);
        return builder.build();
    }

    private void produceLeft(List<KeyValue<Long, String>> keyValues) throws Exception {
        Properties properties = producerConfig();
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, TwoPartitioner.class);
        KafkaProducer<Long, String> producer = new KafkaProducer<>(properties);
        for (KeyValue<Long, String> keyValue : keyValues) {
            producer.send(new ProducerRecord<>(INPUT_TOPIC_LEFT, keyValue.key, keyValue.value)).get();
        }
        producer.close(Duration.ofMillis(0));
    }

    private void produceRight(List<KeyValue<Long, String>> keyValues) throws Exception {
        Properties properties = producerConfig();
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, FourPartitioner.class);
        KafkaProducer<Long, String> producer = new KafkaProducer<>(properties);
        for (KeyValue<Long, String> keyValue : keyValues) {
            producer.send(new ProducerRecord<>(INPUT_TOPIC_RIGHT, keyValue.key, keyValue.value)).get();
        }
        producer.close(Duration.ofMillis(0));
    }

    private List<KeyValue> records(String topic, int partition) {
        KafkaConsumer<Long, String> consumer = new KafkaConsumer<>(consumerConfig());
        consumer.assign(Collections.singleton(new TopicPartition(topic, partition)));
        final ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofSeconds(10));
        List<KeyValue> ret = new ArrayList<>();
        for (ConsumerRecord<Long, String> record : consumerRecords.records(topic)) {
            ret.add(KeyValue.pair(record.key(), record.value()));
        }
        return ret;
    }

    @Test
    public void testMerge() throws Exception {
        // Create KafkaStreams
        KafkaStreams kafkaStreams = new KafkaStreams(topology(), streamsConfig());

        try {
            kafkaStreams.start();

            // Produce left topic: 1 record per partition.
            produceLeft(Arrays.asList(KeyValue.pair(1L, "ein"), KeyValue.pair(2L, "zwei")));

            assertThat(records(INPUT_TOPIC_LEFT, 0), is(Arrays.asList(KeyValue.pair(1L, "ein"))));
            assertThat(records(INPUT_TOPIC_LEFT, 1), is(Arrays.asList(KeyValue.pair(2L, "zwei"))));

            // Produce right topic: 1 record per partition.
            produceRight(Arrays.asList(KeyValue.pair(1L, "one"), KeyValue.pair(2L, "two"), KeyValue.pair(3L, "three"),
                KeyValue.pair(4L, "four")));

            assertThat(records(INPUT_TOPIC_RIGHT, 0), is(Arrays.asList(KeyValue.pair(1L, "one"))));
            assertThat(records(INPUT_TOPIC_RIGHT, 1), is(Arrays.asList(KeyValue.pair(2L, "two"))));
            assertThat(records(INPUT_TOPIC_RIGHT, 2), is(Arrays.asList(KeyValue.pair(3L, "three"))));
            assertThat(records(INPUT_TOPIC_RIGHT, 3), is(Arrays.asList(KeyValue.pair(4L, "four"))));

            // Merge left and right: 1 record for partition 1, rest for partition 0.
            assertThat(records(OUTPUT_TOPIC, 0), containsInAnyOrder(KeyValue.pair(1L, "ein"),
                KeyValue.pair(2L, "zwei"), KeyValue.pair(1L, "one"), KeyValue.pair(2L, "two"), KeyValue.pair(4L, "four")));
            assertThat(records(OUTPUT_TOPIC, 1), is(Arrays.asList(KeyValue.pair(3L, "three"))));
        } finally {
            kafkaStreams.close();
        }
    }
}
