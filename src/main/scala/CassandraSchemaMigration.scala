import java.util.{Date, UUID}

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector._
import org.apache.spark.{SparkContext, SparkConf}

object CassandraSchemaMigration extends App {

  val conf = new SparkConf().set("spark.cassandra.connection.host", "127.0.0.1")
  val sc = new SparkContext("local[2]", "CassandraSchemaMigration", conf)

  CassandraConnector(conf).withSessionDo { session =>
    session.execute("CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
    session.execute("CREATE TABLE IF NOT EXISTS test.customer_events(  customer_id text,   time timestamp, id uuid,  event_type text, store_name text, store_type text, store_location text, staff_name text, staff_title text,  PRIMARY KEY ((customer_id), time, id))")
    session.execute("CREATE TABLE IF NOT EXISTS test.customer_events_by_staff(  customer_id text,   time timestamp, id uuid,  event_type text, store_name text, store_type text, store_location text, staff_name text, staff_title text,  PRIMARY KEY ((staff_name), time, id))")
    session.execute("truncate test.customer_events")
    session.execute("truncate test.customer_events_by_staff")

    session.execute(s"insert into test.customer_events (customer_id, time , id, event_type, staff_name, staff_title, store_location , store_name , store_type ) VALUES ( 'chbatey', ${new Date().getTime}, ${UUID.randomUUID()}, 'BUY_MOVIE', 'Charlie', 'Awesome Marketer', 'WEB', 'ChrisBatey.com', 'US')")
    session.execute(s"insert into test.customer_events (customer_id, time , id, event_type, staff_name, staff_title, store_location , store_name , store_type ) VALUES ( 'chbatey', ${new Date().getTime}, ${UUID.randomUUID()}, 'WATCH_MOVIE', 'Charlie', 'Awesome Marketer', 'WEB', 'ChrisBatey.com', 'US')")
    session.execute(s"insert into test.customer_events (customer_id, time , id, event_type, staff_name, staff_title, store_location , store_name , store_type ) VALUES ( 'chbatey', ${new Date().getTime}, ${UUID.randomUUID()}, 'LOGOUT', 'Charlie', 'Awesome Marketer', 'WEB', 'ChrisBatey.com', 'US')")
  }

  val events_by_customer = sc.cassandraTable("test", "customer_events")
  events_by_customer.saveToCassandra("test", "customer_events_by_staff", SomeColumns("customer_id", "time", "id", "event_type", "staff_name", "staff_title", "store_location", "store_name", "store_type"))

  // or
  //sc.cassandraTable("test", "customer_events").saveToCassandra("test", "customer_events_by_staff", SomeColumns("customer_id", "time", "id", "event_type", "staff_name", "staff_title", "store_location", "store_name", "store_type"))

}
