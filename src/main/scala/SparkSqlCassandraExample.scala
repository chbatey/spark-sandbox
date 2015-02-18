import java.util.{UUID, Date}

import com.datastax.spark.connector.rdd._
import org.apache.spark._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.cassandra.CassandraSQLContext

object SparkSqlCassandraExample extends App {
  val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1")
  val sc = new SparkContext("local[4]", "test", conf)

  CassandraConnector(conf).withSessionDo { session =>
    session.execute("CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
    session.execute("CREATE TABLE IF NOT EXISTS test.customer_events(  customer_id text,   time timestamp, id uuid,  event_type text, store_name text, store_type text, store_location text, staff_name text, staff_title text,  PRIMARY KEY ((customer_id), time, id))")
    session.execute("truncate test.customer_events")
    session.execute(s"insert into test.customer_events (customer_id, time , id, event_type, staff_name, staff_title, store_location , store_name , store_type ) VALUES ( 'chbatey', ${new Date().getTime}, ${UUID.randomUUID()}, 'BUY_MOVIE', 'Charlie', 'Awesome Marketer', 'WEB', 'ChrisBatey.com', 'US')")
    session.execute(s"insert into test.customer_events (customer_id, time , id, event_type, staff_name, staff_title, store_location , store_name , store_type ) VALUES ( 'chbatey', ${new Date().getTime}, ${UUID.randomUUID()}, 'WATCH_MOVIE', 'Charlie', 'Awesome Marketer', 'WEB', 'ChrisBatey.com', 'US')")
    session.execute(s"insert into test.customer_events (customer_id, time , id, event_type, staff_name, staff_title, store_location , store_name , store_type ) VALUES ( 'chbatey', ${new Date().getTime}, ${UUID.randomUUID()}, 'LOGOUT', 'Ted', 'Awesome Marketer', 'WEB', 'ChrisBatey.com', 'US')")

    session.execute(s"insert into test.customer_events (customer_id, time , id, event_type, staff_name, staff_title, store_location , store_name , store_type ) VALUES ( 'chbatey', ${new Date().getTime}, ${UUID.randomUUID()}, 'LOGIN', 'Charlie', 'Awesome Marketer', 'APP', 'SportsApp', 'US')")
    session.execute(s"insert into test.customer_events (customer_id, time , id, event_type, staff_name, staff_title, store_location , store_name , store_type ) VALUES ( 'chbatey', ${new Date().getTime}, ${UUID.randomUUID()}, 'WATCH_STREAM', 'Charlie', 'Awesome Marketer', 'APP', 'SportsApp', 'US')")
    session.execute(s"insert into test.customer_events (customer_id, time , id, event_type, staff_name, staff_title, store_location , store_name , store_type ) VALUES ( 'chbatey', ${new Date().getTime}, ${UUID.randomUUID()}, 'WATCH_MOVIE', 'Charlie', 'Awesome Marketer', 'APP', 'SportsApp', 'US')")
    session.execute(s"insert into test.customer_events (customer_id, time , id, event_type, staff_name, staff_title, store_location , store_name , store_type ) VALUES ( 'chbatey', ${new Date().getTime}, ${UUID.randomUUID()}, 'WATCH_MOVIE', 'Charlie', 'Awesome Marketer', 'APP', 'SportsApp', 'US')")
    session.execute(s"insert into test.customer_events (customer_id, time , id, event_type, staff_name, staff_title, store_location , store_name , store_type ) VALUES ( 'chbatey', ${new Date().getTime}, ${UUID.randomUUID()}, 'LOGOUT', 'Ted', 'Awesome Marketer', 'APP', 'SportsApp', 'US')")
  }

  val cc = new CassandraSQLContext(sc)
  cc.setKeyspace("test")
  val rdd: SchemaRDD = cc.sql("SELECT store_name, event_type, count(store_name) from customer_events GROUP BY store_name, event_type")
  rdd.collect().foreach(println)
}
