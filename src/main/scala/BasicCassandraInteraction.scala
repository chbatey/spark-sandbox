import com.datastax.spark.connector.rdd.CassandraRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector

object BasicCassandraInteraction extends App {
  val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1")
  val sc = new SparkContext("local[4]", "test", conf)

  CassandraConnector(conf).withSessionDo { session =>
    session.execute("CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
    session.execute("CREATE TABLE IF NOT EXISTS test.kv(key text PRIMARY KEY, value int)")
    session.execute("INSERT INTO test.kv(key, value) VALUES ('key1', 1)")
    session.execute("INSERT INTO test.kv(key, value) VALUES ('key2', 2)")
  }

  val rdd: CassandraRDD[CassandraRow] = sc.cassandraTable("test", "kv")
  println(rdd.count())
  println(rdd.first())
  val values: RDD[Int] = rdd.map(row => row.getInt("value"))
  println(values.max())
}
