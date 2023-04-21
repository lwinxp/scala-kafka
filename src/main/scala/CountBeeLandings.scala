import org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG
import org.apache.kafka.streams.kstream.{Named, SlidingWindows, TimeWindows}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

import scala.collection.mutable
import java.time.Duration
import java.util.Properties

object CountBeeLandings extends App {
  import Serdes._
  val props = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "count-application")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(AUTO_OFFSET_RESET_CONFIG, "earliest")
  props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass)
  props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass)
  props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0)

  def makeTopology(T: Int, outputTopic: String) = {
    val count = mutable.Map.empty[String, Int].withDefaultValue(0)
    var setFirstWindow = false
    var latestWindow = ""
    var windowChanged = false

    val builder = new StreamsBuilder
    builder
      .stream[String, String]("events")
      .groupByKey
      .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(T)))
      .aggregate("0")((key, value, ag) => {
        val xcoord = value.split(",")(2)
        val ycoord = value.split(",")(3)
        val coordinate = s"($xcoord,$ycoord)"

        count(coordinate) += 1
        count.mkString("")
      })
      .toStream
      .mapValues((windowedKey, result) => {

        if (!setFirstWindow) {
          latestWindow = s"${windowedKey.window()}"
          setFirstWindow = true
        }

        if (windowChanged) {
          count.clear()
        }

        if (latestWindow != s"${windowedKey.window()}") {
          latestWindow = s"${windowedKey.window()}"
          windowChanged = true
          result.mkString("") // output the counts for each coordinate for the window
        } else {
          windowChanged = false
          null
        }

      })
      .filter((_, result) => result != null)
      .to(outputTopic)

    builder.build()
  }

  // NOTE: Set argument for time window interval T here.
  val streams: KafkaStreams = new KafkaStreams(makeTopology(10, "bee-counts"), props)

  streams.start()

  sys.ShutdownHookThread {
    streams.close(Duration.ofSeconds(10))
  }
}
