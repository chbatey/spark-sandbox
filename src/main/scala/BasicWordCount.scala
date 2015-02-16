import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

object BasicWordCount extends App {
  val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1")
  val sc = new SparkContext("local[4]", "test", conf)

  CassandraConnector(conf).withSessionDo { session =>
    session.execute("CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
    session.execute("CREATE TABLE IF NOT EXISTS test.words(word text PRIMARY KEY, count int)")
  }

  val textFile: RDD[String] = sc.textFile("Spark-Readme.md")

  val words: RDD[String] = textFile.flatMap(line => line.split("\\s+"))
  val wordAndCount: RDD[(String, Int)] = words.map((_, 1))
  val wordCounts: RDD[(String, Int)] = wordAndCount.reduceByKey(_ + _)

  println(wordCounts.first())

  wordCounts.filter(_._1 == null).saveToCassandra("test", "words", SomeColumns("word", "count"))
}
