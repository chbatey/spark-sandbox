import com.datastax.spark.connector.rdd._
import org.apache.spark._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._

object BasicCassandraInteraction extends App {
  val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1")
  val sc = new SparkContext("local[4]", "test", conf)

  CassandraConnector(conf).withSessionDo { session =>
    session.execute("CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
    session.execute("CREATE TABLE IF NOT EXISTS test.kv(key text PRIMARY KEY, value int)")
    session.execute("INSERT INTO test.kv(key, value) VALUES ('chris', 10)")
    session.execute("INSERT INTO test.kv(key, value) VALUES ('dan', 1)")
    session.execute("INSERT INTO test.kv(key, value) VALUES ('charlieS', 2)")
  }

  val rdd: CassandraRDD[CassandraRow] = sc.cassandraTable("test", "kv")
  println(rdd.count())
  println(rdd.first())
  println(rdd.max()(new Ordering[CassandraRow] {
    override def compare(x: CassandraRow, y: CassandraRow): Int = x.getInt("value").compare(y.getInt("value"))
  }))
}
