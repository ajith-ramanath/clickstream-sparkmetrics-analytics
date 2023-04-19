/* 
    Read the data from the Kafka topic and process it using Flink in Scala 
 */

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows

// Declare main
def main(args: Array[String]) {

    // Read the configuration file located in the resources folder
    val config = ConfigFactory.load("application.conf")

    // Set the Kafka properties
    val properties = new java.util.Properties()
    properties.setProperty("bootstrap.servers", config.getString("kafka.bootstrap.servers"))
    properties.setProperty("group.id", config.getString("kafka.group.id"))
    properties.setProperty("zookeeper.connect", config.getString("kafka.zookeeper.connect"))
    
    // Set the topic name to read from
    val readTopic = config.getString("kafka.read.topic")

    // Set the topic name to write to
    val writeTopic = config.getString("kafka.write.topic")

    // Create a StreamExecutionEnvironment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // Create a Kafka Consumer
    val kafkaConsumer = new FlinkKafkaConsumer[String]("test", new SimpleStringSchema(), properties)

    // Create a DataStream
    val stream = env.addSource(kafkaConsumer)

    // Process the json data, split into different tables based on the event type
    val processedStream = stream
        .map { x => 
            val json = parse(x)
            val eventType = (json \ "event_type").extract[String]
            val eventTime = (json \ "event_time").extract[String]
            val event = (json \ "event").extract[String]
            (eventType, eventTime, event)
        }
        .keyBy(0)
        .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
        .apply { (key, window, events, out: Collector[(String, String, String)]) => 
            val eventType = key
            val eventTime = events.map(_._2).mkString(",")
            val event = events.map(_._3).mkString(",")
            out.collect((eventType, eventTime, event))
        }
    

    // Print the result
    processedStream.print()

    // Execute the program
    env.execute("Flink Streaming Scala API Skeleton")
}
