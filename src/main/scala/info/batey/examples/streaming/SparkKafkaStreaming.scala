package info.batey.examples.streaming

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.EmbeddedKafka
import com.datastax.spark.connector.streaming._
import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark._
import org.json4s._
import org.json4s.ext.UUIDSerializer
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization._

object SparkKafkaStreaming {
  implicit val formats = DefaultFormats + UUIDSerializer


  def main(args: Array[String]): Unit = {

    val joeBuy = write(CustomerEvent("joe", "chris", "WEB", "NEW_CUSTOMER", "lots of fancy content", event_type = "BUY"))
    val joeBuy2 = write(CustomerEvent("joe", "chris", "WEB", "NEW_CUSTOMER", "lots of fancy content", event_type = "BUY"))
    val joeSell = write(CustomerEvent("joe", "chris", "WEB", "NEW_CUSTOMER", "lots of fancy content", event_type = "SELL"))
    val chrisBuy = write(CustomerEvent("chris", "chris", "WEB", "NEW_CUSTOMER", "lots of fancy content", event_type = "BUY"))

    val topic = "Events"
    val kafka = new EmbeddedKafka
    kafka.createTopic(topic)
    kafka.produceAndSendMessage(topic, Map(joeBuy -> 1))
    kafka.produceAndSendMessage(topic, Map(joeBuy2 -> 1))
    kafka.produceAndSendMessage(topic, Map(joeSell -> 1))
    kafka.produceAndSendMessage(topic, Map(chrisBuy -> 1))

    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "localhost")
    val sc = new SparkContext("local[2]", "test", conf)
    val ssc = new StreamingContext(sc, Seconds(10))

    CassandraConnector(conf).withSessionDo { session =>
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS streaming WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
      session.execute("CREATE TABLE IF NOT EXISTS streaming.customer_events_by_type ( nameAndType text primary key, number int)")
      session.execute("CREATE TABLE IF NOT EXISTS streaming.customer_events ( " +
        "customer_id text, " +
        "staff_id text, " +
        "store_type text, " +
        "group text static, " +
        "content text,   " +
        "time timeuuid,  " +
        "event_type text,  " +
        "PRIMARY KEY ((customer_id), time) )")
    }

    val rawEvents: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafka.kafkaParams, Map(topic -> 1), StorageLevel.MEMORY_ONLY)
    val events: DStream[CustomerEvent] = rawEvents.map({ case (k, v) =>
      parse(v).extract[CustomerEvent]
    })

    events.print()
    events.saveToCassandra("streaming", "customer_events")
    val eventsByCustomerAndType = events.map(event => (s"${event.customer_id}-${event.event_type}", 1)).reduceByKey(_ + _)
    eventsByCustomerAndType.print()
    eventsByCustomerAndType.saveToCassandra("streaming", "customer_events_by_type")

    ssc.start()
    ssc.awaitTermination()
  }

}
