package info.batey.examples.streaming

import com.datastax.driver.core.utils.UUIDs
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.streaming._
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming._

object BasicSparkStreamingSocket extends App with LazyLogging {
  val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
  val ssc = new StreamingContext(conf, Seconds(5))

  CassandraConnector(conf).withSessionDo { session =>
    session.execute("CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
    session.execute("CREATE TABLE IF NOT EXISTS test.network_word_count(word text PRIMARY KEY, number int)")
    session.execute("CREATE TABLE IF NOT EXISTS test.network_word_count_raw(time timeuuid PRIMARY KEY, raw text)")
  }

  val lines = ssc.socketTextStream("localhost", 9999)
  lines.map((UUIDs.timeBased(), _)).saveToCassandra("test", "network_word_count_raw")

  val words = lines.flatMap(_.split("\\s+"))
  val countOfOne = words.map((_, 1))
  val reduced = countOfOne.reduceByKey(_ + _)
  reduced.saveToCassandra("test", "network_word_count")
  reduced.print()

  ssc.start()
  ssc.awaitTermination()
}
