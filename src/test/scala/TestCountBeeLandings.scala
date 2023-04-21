import org.apache.kafka.streams.scala.serialization.Serdes
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._
import org.apache.kafka.streams.{StreamsConfig, TopologyTestDriver}
import org.apache.kafka.streams.test.TestRecord

import scala.jdk.CollectionConverters._
import java.time.Instant

class TestGenerateBeeFlight extends AnyFunSuite {
  import Serdes.stringSerde
  import CountBeeLandings.makeTopology

  test("The generated bee flight solution is correct") {
    val props = new java.util.Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")

    val testDriver = new TopologyTestDriver(makeTopology(5, "bee-counts"), props)

    val testInputTopic = testDriver.createInputTopic("events", stringSerde.serializer(), stringSerde.serializer())
    val testOutputTopic = testDriver.createOutputTopic("bee-counts", stringSerde.deserializer(), stringSerde.deserializer())

    val inputVals = List(
      new TestRecord("1681970445", "aff7ccb8-7886-48d2-9f29-4fd095b45261,1681970445,1,0", Instant.ofEpochMilli(1681970410000L)),
      new TestRecord("1681970510", "c819e54d-7706-4697-9d74-f805128182fe,1681970510,1,0", Instant.ofEpochMilli(1681970411000L)),
      new TestRecord("1681970575", "aff7ccb8-7886-48d2-9f29-4fd095b45261,1681970575,1,1", Instant.ofEpochMilli(1681970412000L)),
      new TestRecord("1681970640", "c819e54d-7706-4697-9d74-f805128182fe,1681970640,1,0", Instant.ofEpochMilli(1681970413000L)),
      new TestRecord("1681970705", "c819e54d-7706-4697-9d74-f805128182fe,1681970705,0,1", Instant.ofEpochMilli(1681970414000L)),
      new TestRecord("1681970770", "aff7ccb8-7886-48d2-9f29-4fd095b45261,1681970770,1,1", Instant.ofEpochMilli(1681970415000L)),
    )

    testInputTopic.pipeRecordList(inputVals.asJava)

    val outputVals = List(
      "(1,1) -> 2(0,1) -> 1(1,0) -> 3"
    )

    testOutputTopic.readValuesToList().asScala shouldBe outputVals

  }
}