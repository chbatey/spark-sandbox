import java.util.UUID

import com.datastax.driver.core.utils.UUIDs
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.EmbeddedKafka
import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{ReceiverInputDStream, DStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkEnv, SparkContext, SparkConf}
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.sql.{Row, SchemaRDD}
import StreamingContext._
import com.datastax.spark.connector.streaming._
import org.json4s._
import org.json4s.ext.UUIDSerializer
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization._

object SparkKafkaStreaming {
  implicit val formats = DefaultFormats + UUIDSerializer


  def main(args: Array[String]): Unit = {

    val customerEvent = CustomerEvent("joe", "chris", "WEB", "NEW_CUSTOMER", "lots of fancy content")
    val serialisedEvent: String = write(customerEvent)
    val topic = "Events"
    //    println("starting an embedded kafka")
    //    val kafka = new EmbeddedKafka
    //    kafka.createTopic(topic)
    //    kafka.produceAndSendMessage(topic, Map(serialisedEvent -> 1))

    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "localhost")
    //    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "192.168.10.11")
    //    val sc = new SparkContext("spark://192.168.10.11:7077", "test", conf)
    val sc = new SparkContext("local[2]", "test", conf)
    val ssc = new StreamingContext(sc, Seconds(30))

    CassandraConnector(conf).withSessionDo { session =>
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS events WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
      session.execute("CREATE TABLE  IF NOT EXISTS events.customer_events ( " +
        "customer_id text, " +
        "staff_id text, " +
        "store_type text, " +
        "group text static, " +
        "content text,   " +
        "time timeuuid,  " +
        "event_type text,  " +
        "PRIMARY KEY ((customer_id), time) )")
    }


    //    val rawEvents: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.10.11", 9090)
    //    val rawEvents: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafka.kafkaParams, Map(topic -> 1), StorageLevel.MEMORY_ONLY)

    val kafkaParams: Map[String, String] = Map(
      "zookeeper.connect" -> "192.168.86.5:2181",
      "group.id" -> "consumer-spark",
      "auto.offset.reset" -> "smallest")

    val rawEvents: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Map(topic -> 1), StorageLevel.MEMORY_ONLY)

    val events: DStream[CustomerEvent] = rawEvents.map({ case (k, v) => {
      parse(v).extract[CustomerEvent]
    }})

    events.saveToCassandra("events", "customer_events")

    ssc.start()
    ssc.awaitTermination()
  }

}
