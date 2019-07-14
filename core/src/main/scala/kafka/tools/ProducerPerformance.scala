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

import java.io.File
import java.util.Properties

import com.typesafe.scalalogging.LazyLogging
import kafka.utils.CommandLineUtils
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.utils.Utils

import scala.collection.JavaConverters._

/**
 * Performance test for the full zookeeper producer
 */
object ProducerPerformance extends LazyLogging {

  def main(args: Array[String]): Unit = {
    val config = new ProducerPerfConfig(args)
  }

  class ProducerPerfConfig(args: Array[String]) extends PerfConfig(args) {
    val bootstrapServerOpt = parser.accepts("bootstrap-server", "REQUIRED: The server(s) to connect to.")
      .withRequiredArg
      .describedAs("bootstrap-server")
      .ofType(classOf[String])
      .required
    val topicOpt = parser.accepts("topic", "REQUIRED: Produce messages to this topic.")
      .withRequiredArg
      .describedAs("topic")
      .ofType(classOf[String])
      .required
    val numRecordsOpt = parser.accepts("num-records", "REQUIRED: Number of messages to produce.")
      .withRequiredArg
      .describedAs("count")
      .ofType(classOf[java.lang.Long])
    val recordSizeOpt = parser.accepts("record-size",
      "Message size in bytes."
        + " Note that you must provide exactly one of --record-size or --payload-file.")
      .withRequiredArg
      .describedAs("size")
      .ofType(classOf[Integer])
    val payloadFileOpt = parser.accepts("payload-file",
      "File to read the message payloads from. This works only for UTF-8 encoded text files."
        + " Payloads will be read from this file and a payload will be randomly selected when sending messages."
        + " Note that you must provide exactly one of --record-size or --payload-file.")
      .availableUnless("record-size")
      .withRequiredArg
      .describedAs("payload file")
      .ofType(classOf[File])
    val payloadDelimiterOpt = parser.accepts("payload-delimiter",
      "Provides delimiter to be used when --payload-file is provided. Defaults to new line."
        + " Note that this parameter will be ignored if --payload-file is not provided.")
      .availableIf("payload-file")
      .withRequiredArg
      .describedAs("payload-delimiter")
      .defaultsTo("\\n")
      .ofType(classOf[String])
    val throughputOpt = parser.accepts("throughput",
      "REQUIRED: Throttle maximum message throughput to *approximately* THROUGHPUT messages/sec."
        + " Set this to -1 to disable throttling.")
      .withRequiredArg
      .describedAs("throughput")
      .ofType(classOf[Integer])
      .required
    val producerPropsOpt = parser.accepts("producer-props",
      "Kafka producer related configuration properties like bootstrap.servers,client.id etc."
        + " These configs take precedence over those passed via --producer.config.")
      .withRequiredArg
      .describedAs("PROP-NAME=PROP-VALUE")
      .ofType(classOf[String])
    val producerConfigOpt = parser.accepts("producer.config", "Producer config properties file.")
      .withRequiredArg
      .describedAs("config file")
      .ofType(classOf[String])
    val printMetricsOpt = parser.accepts("print-metrics", "Print out metrics at the end of the test.")
      .withOptionalArg
      .describedAs("print metrics")
      .ofType(classOf[java.lang.Boolean])
    val transactionalIdOpt = parser.accepts("transactional-id",
      "The transactionalId to use if transaction-duration-ms is > 0. Useful when testing the performance of concurrent transactions.")
      .withRequiredArg
      .describedAs("transactional id")
      .ofType(classOf[String])
      .defaultsTo("performance-producer-default-transactional-id")
    val transactionDurationMsOpt = parser.accepts("transaction-duration-ms",
      "The max age of each transaction. The commitTransaction will be called after this time has elapsed."
        + " Transactions are only enabled if this value is positive.")
      .withRequiredArg
      .describedAs("milliseconds")
      .ofType(classOf[java.lang.Long])
      .defaultsTo(0L)

    options = parser.parse(args: _*)

    CommandLineUtils.printHelpAndExitIfNeeded(this, "This tool helps in performance test for the full zookeeper producer")

    val printMetrics = options.has(printMetricsOpt)

    // backward compatibility: numMessagesOpt, numRecordsOpt
    if(options.has(numMessagesOpt) == options.has(numRecordsOpt))
      throw new IllegalArgumentException("Only one of --messages or --num-records must be specified")

    val numMessages = if (options.has(numMessagesOpt)) {
      options.valueOf(numMessagesOpt).longValue
    } else {
      println("DEPRECATED: --num-records is now deprecated. Use --messages instead.")
      options.valueOf(numRecordsOpt).longValue
    }

    val topic = options.valueOf(topicOpt)

    if(options.has(recordSizeOpt) == options.has(payloadFileOpt))
      throw new IllegalArgumentException("Only one of --record-size or --payload-file must be specified")

    val recordSize = if (options.has(recordSizeOpt)) {
      require(options.valueOf(recordSizeOpt).intValue > 0)
      options.valueOf(recordSizeOpt).intValue
    } else {
      0
    }

    val throughput = options.valueOf(throughputOpt).intValue

    val props = if (options.has(producerConfigOpt))
      Utils.loadProps(options.valueOf(producerConfigOpt))
    else
      new Properties

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, options.valueOf(bootstrapServerOpt))
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    if (options.valueOf(transactionDurationMsOpt).longValue > 0L) {
      props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, options.valueOf(transactionalIdOpt))
    }
  }

}
