import java.sql.{DriverManager, ResultSet}
import java.util.UUID

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark._
import org.apache.spark.rdd.JdbcRDD

object RdmsToCassandra extends App {

  val conf = new SparkConf().set("spark.cassandra.connection.host", "127.0.0.1")
  val sc = new SparkContext("local[2]", "MigrateMySQLToCassandra", conf)
  val mysqlJdbcString: String = s"jdbc:mysql://192.168.10.11/customer_events?user=root&password=password"
  Class.forName("com.mysql.jdbc.Driver").newInstance

  CassandraConnector(conf).withSessionDo { session =>
    session.execute("CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
    session.execute("CREATE TABLE IF NOT EXISTS test.customer_events(  customer_id text,   time timestamp, id uuid,  event_type text, " +
      "store_name text, store_type text, store_location text, staff_name text, staff_title text,  PRIMARY KEY ((customer_id), time, id))")
  }

  val highestId: Long = 1000
  val startingId: Long = 0
  val numberOfPartitions = 6;
  val customerEvents = new JdbcRDD(sc, () => { DriverManager.getConnection(mysqlJdbcString)},
    "select * from customer_events ce, staff, store where ce.store = store.store_name and ce.staff = staff.name " +
        "and ce.id >= ? and ce.id <= ?", startingId, highestId, numberOfPartitions,
    (r: ResultSet) => {
      (r.getString("customer"),
        r.getTimestamp("time"),
        UUID.randomUUID(),
        r.getString("event_type"),
        r.getString("store_name"),
        r.getString("location"),
        r.getString("store_type"),
        r.getString("staff"),
        r.getString("job_title")
        )
    })

  customerEvents.saveToCassandra("test", "customer_events",
      SomeColumns("customer_id", "time", "id", "event_type", "store_name", "store_type", "store_location", "staff_name", "staff_title"))
}
