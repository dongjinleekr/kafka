/**
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

package kafka.tools

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{ByteArraySerializer, LongSerializer}
import org.hamcrest.CoreMatchers.equalTo
import org.hamcrest.MatcherAssert.assertThat
import org.junit.Test

class ProducerPerformanceTest {
  def args(str: String): Array[String] = {
    str.split(" ")
  }

  @Test
  def testParsing(): Unit = {
    val parameters = "--bootstrap-server localhost:9092 --topic performance --messages 42 --record-size 1000" +
      " --throughput -1 --print-metrics"

    val config = new ProducerPerformance.ProducerPerfConfig(args(parameters))
    assertThat(config.topic, equalTo("performance"))
    assertThat(config.numMessages, equalTo(42L))
    assertThat(config.recordSize, equalTo(1000))
    assertThat(config.throughput, equalTo(-1))
    assertThat(config.props.get("bootstrap.servers").asInstanceOf[String], equalTo("localhost:9092"))
    assertThat(config.props.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG).asInstanceOf[String],
      equalTo(classOf[ByteArraySerializer].getName))
    assertThat(config.props.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG).asInstanceOf[String],
      equalTo(classOf[ByteArraySerializer].getName))
    assertThat(config.printMetrics, equalTo(true))
  }

  @Test
  def testNumRecords(): Unit = {
    val parameters = "--bootstrap-server localhost:9092 --topic performance --num-records 42 --record-size 1000" +
      " --throughput -1 --print-metrics"

    val config = new ProducerPerformance.ProducerPerfConfig(args(parameters))
    assertThat(config.topic, equalTo("performance"))
    assertThat(config.numMessages, equalTo(42L))
    assertThat(config.recordSize, equalTo(1000))
    assertThat(config.throughput, equalTo(-1))
    assertThat(config.props.get("bootstrap.servers").asInstanceOf[String], equalTo("localhost:9092"))
    assertThat(config.props.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG).asInstanceOf[String],
      equalTo(classOf[ByteArraySerializer].getName))
    assertThat(config.props.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG).asInstanceOf[String],
      equalTo(classOf[ByteArraySerializer].getName))
    assertThat(config.printMetrics, equalTo(true))
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testMessagesMissing(): Unit = {
    // Either of 'num-records' or 'messages' must be specified
    val parameters = "--bootstrap-server localhost:9092 --topic performance --record-size 1000 --throughput -1" +
      " --print-metrics"

    val _ = new ProducerPerformance.ProducerPerfConfig(args(parameters))
  }
}
