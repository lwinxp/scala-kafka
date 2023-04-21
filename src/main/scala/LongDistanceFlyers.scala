import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import scala.collection.mutable.ListBuffer
import scala.collection.mutable

import java.util.Properties
import java.time.Duration
import org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG

object LongDistanceFlyers extends App {
  import Serdes._
  
  val props = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "traveller-application")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(AUTO_OFFSET_RESET_CONFIG, "earliest")
  props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass)
  props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass)
  props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0)

  def makeTopology(k : Int, outputTopic: String) = {
    val count = mutable.Map.empty[String, Int].withDefaultValue(0)
    val published = ListBuffer.empty[String]
    val builder = new StreamsBuilder

    builder
      .stream[String, String]("events")
      .groupByKey
      .aggregate(initializer = "0")((key, value, initializer) => {
        val guid = value.split(",")(0)
        count(guid) += 1
        if (count(guid) == k && !published.contains(guid)) {
          published += guid
          s"${guid}"
        } else {
          null
        }
      })
      .toStream
      .filter((_, value) => value != null) // filter out null records
      .mapValues((window, value) => {
        value.mkString("") // output the counts for each coordinate for the window
      })
      .to("long-distance-travellers")

    builder.build()
  }

  // NOTE: Set argument for K here.
  val streams: KafkaStreams = new KafkaStreams(makeTopology(10, "long-distance-travellers"), props)
  streams.start()
  
  sys.ShutdownHookThread {
    streams.close(Duration.ofSeconds(10))
  }
  
}
