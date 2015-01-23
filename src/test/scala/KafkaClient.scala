import java.net.InetAddress
import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import kafka.serializer.StringEncoder
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.json4s._
import org.json4s.ext.UUIDSerializer
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write


object KafkaClient extends App {

  val ZookeeperConnectionString = "192.168.86.5:2181"
  val BrokerConnectionString = "192.168.86.10:9092"


  implicit val formats = Serialization.formats(NoTypeHints) + UUIDSerializer

  val producerConfig: ProducerConfig = {
    val p = new Properties()
    p.put("metadata.broker.list", BrokerConnectionString)
    p.put("serializer.class", classOf[StringEncoder].getName)
    new ProducerConfig(p)
  }

  println(s"Connecting to zookeeper $ZookeeperConnectionString")
  val client = new ZkClient(ZookeeperConnectionString, 3000, 3000, ZKStringSerializer)
  println(s"ZooKeeper Client connected.")

  val producer = new Producer[String, String](producerConfig)

  val customerEvent = CustomerEvent("joe", "chris", "WEB", "NEW_CUSTOMER", "lots of fancy content")
  val serialisedEvent: String = write(customerEvent)

  println(s"Sending message $serialisedEvent")

  producer.send(new KeyedMessage[String, String]("Events", "key", serialisedEvent))

  producer.close()
}
