import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties
import java.util.UUID
import scala.util.Random
import java.time.Instant

object GenerateBeeFlight extends App {
  val props: Properties = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "flight-application")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  props.put("acks", "all")
  props.put("linger.ms", 1)
  props.put("retries", 0)

  // NOTE: Set arguments for maxX (W), maxY (H) here.
  // Consider to set no. of x to 1 and y to 1, which is easier for testing and observation purposes
  val (maxX, maxY) = args match {
    case Array(w: String, h: String) => (w.toInt, h.toInt)
    case _ => (1, 1)
  }

  val producer: Producer[String, String] = new KafkaProducer[String, String](props)
  val topic = "events"

  val epochTime = Instant.now.getEpochSecond
  var timestamp = epochTime

  // NOTE: Set arguments for no. of bees here if desired.
  // Consider to set no. of bees as 2 which is easier for testing and observation purposes
  // No. of bees is random from 10 - 100 individuals according to readme instructions
  val randNum10To100 = 10 + Random.nextInt(91)
//  val randNum10To100 = 2

  val guidList = List.fill(randNum10To100)(UUID.randomUUID())

  while(true) {
    val advance = Random.nextInt(1001)
    timestamp = timestamp + advance

    val randomGuidIndex = Random.nextInt(guidList.length)
    val randomGuid = guidList(randomGuidIndex)

    val randX = Random.nextInt(maxX + 1)
    val randY = Random.nextInt(maxY + 1)

    val output = s"$randomGuid,$timestamp,$randX,$randY"
    producer.send(new ProducerRecord[String, String](topic,s"$timestamp",output))
    println(output)
    Thread.sleep(Random.nextInt(49) + 1)
//    Thread.sleep(1000)

  }

  sys.ShutdownHookThread {
    producer.flush()
    producer.close()
  }
}
