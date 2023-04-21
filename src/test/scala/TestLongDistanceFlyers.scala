import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.{KeyValue, StreamsConfig, TopologyTestDriver}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import scala.jdk.CollectionConverters._

class TestLongDistanceFlyers extends AnyFunSuite {
  import LongDistanceFlyers.makeTopology
  import Serdes.stringSerde

  test("The generated long distance flyers solution is correct") {
    val props = new java.util.Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")

    val testDriver = new TopologyTestDriver(makeTopology(5,"long-distance-travellers"), props)

    val testInputTopic = testDriver.createInputTopic("events", stringSerde.serializer(), stringSerde.serializer())
    val testOutputTopic = testDriver.createOutputTopic("long-distance-travellers", stringSerde.deserializer(), stringSerde.deserializer())

    val inputVals = List(
      new KeyValue("1681983517", "1e28a496-f80c-4a7d-8ee4-e4717f33db5f,1681983517,1,0"),
      new KeyValue("1681984256", "35c70c7e-4aa1-4e85-9bbe-d7dfba2682ed,1681984256,1,0"),
      new KeyValue("1681984995", "35c70c7e-4aa1-4e85-9bbe-d7dfba2682ed,1681984995,1,0"),
      new KeyValue("1681985734", "35c70c7e-4aa1-4e85-9bbe-d7dfba2682ed,1681985734,1,1"),
      new KeyValue("1681986473", "1e28a496-f80c-4a7d-8ee4-e4717f33db5f,1681986473,0,0"),
      new KeyValue("1681987212", "1e28a496-f80c-4a7d-8ee4-e4717f33db5f,1681987212,1,1"),
      new KeyValue("1681987951", "1e28a496-f80c-4a7d-8ee4-e4717f33db5f,1681987951,0,0"),
      new KeyValue("1681988690", "35c70c7e-4aa1-4e85-9bbe-d7dfba2682ed,1681988690,1,0"),
      new KeyValue("1681989429", "35c70c7e-4aa1-4e85-9bbe-d7dfba2682ed,1681989429,1,0"),
      new KeyValue("1681990168", "1e28a496-f80c-4a7d-8ee4-e4717f33db5f,1681990168,0,0"),
      new KeyValue("1681990907", "35c70c7e-4aa1-4e85-9bbe-d7dfba2682ed,1681990907,1,1"),
      new KeyValue("1681991646", "1e28a496-f80c-4a7d-8ee4-e4717f33db5f,1681991646,0,0"),
    )

    testInputTopic.pipeKeyValueList(inputVals.asJava)

    val outputVals = List(
      "35c70c7e-4aa1-4e85-9bbe-d7dfba2682ed",
      "1e28a496-f80c-4a7d-8ee4-e4717f33db5f",
    )

    testOutputTopic.readValuesToList().asScala shouldBe outputVals

  }
}