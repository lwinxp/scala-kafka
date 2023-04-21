import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.streams.StreamsConfig

import java.sql.DriverManager
import java.time.Duration
import scala.jdk.CollectionConverters._
import java.util.Properties

object SaveLongDistanceFlyers extends App {
  val dbConn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", "postgres", "abc123")
  val createTable = dbConn.prepareStatement(s"create table if not exists longdistancetravellers(longdistancetraveller varchar(1000))")
  createTable.execute()

  val props: Properties = new Properties()
  props.put(ConsumerConfig.GROUP_ID_CONFIG, "storage-application")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  props.setProperty("enable.auto.commit", "true")
  props.setProperty("auto.commit.interval.ms", "1000")
  
  val consumer = new KafkaConsumer(props)
  consumer.subscribe(Seq("long-distance-travellers").asJava)
  while (true) {
    val records = consumer.poll(Duration.ofMillis(300))

    records.records("long-distance-travellers").asScala.foreach { record =>
      val q = s"insert into longdistancetravellers(longdistancetraveller) values('${record.value()}') on conflict do nothing"
      val ps = dbConn.prepareStatement(q)
      println(q)
      ps.execute()
    }
  }

  sys.ShutdownHookThread {
    consumer.close()
  }
}
