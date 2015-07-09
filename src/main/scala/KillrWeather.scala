import com.datastax.spark.connector._
import org.apache.spark.{SparkContext, _}
import org.apache.spark.util.StatCounter

object KillrWeather extends App {
  val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1")
  val sc = new SparkContext("local[4]", "test", conf)

  val wholeTable: Array[Double] = sc.cassandraTable[Double]("isd_weather_data", "raw_weather_data")
              .select("temperature")
              .collect()
  println(StatCounter(wholeTable))

  val singleWeatherStation: Array[Double] = sc.cassandraTable[Double]("isd_weather_data", "raw_weather_data")
    .select("temperature").where("wsid = ?", "725030:14732")
    .collect()
  println(StatCounter(singleWeatherStation))
}
