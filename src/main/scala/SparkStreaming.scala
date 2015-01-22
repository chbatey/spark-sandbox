import java.util.UUID

import com.datastax.driver.core.utils.UUIDs
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.streaming.dstream.{ReceiverInputDStream, DStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.sql.{Row, SchemaRDD}
import StreamingContext._
import com.datastax.spark.connector.streaming._
import org.json4s._
import org.json4s.ext.UUIDSerializer
import org.json4s.jackson.JsonMethods._

object SparkStreaming {
  implicit val formats = DefaultFormats + UUIDSerializer

  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "192.168.10.11")
    val sc = new SparkContext("spark://192.168.10.11:7077", "test", conf)
//    val sc = new SparkContext("local[2]", "test", conf)
    val ssc = new StreamingContext(sc, Seconds(30))

    CassandraConnector(conf).withSessionDo { session =>
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS customerEvents WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
      session.execute("CREATE TABLE  IF NOT EXISTS test.customer_events ( " +
        "customerId text, " +
        "staffId text, " +
        "storeType text, " +
        "group text static,   " +
        "content text,   " +
        "time timeuuid,  " +
        "PRIMARY KEY ((customerId), time) )")
    }


    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.10.11", 9090)

    val events: DStream[CustomerEvent] = lines.map(parse(_).extract[CustomerEvent])

    events.saveToCassandra("test", "customer_events")

    ssc.start()
    ssc.awaitTermination()
  }

}
