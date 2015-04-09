
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.{SparkContext, SparkConf}

object EventsAggregations  {

  /*
 * This is the entry point for the application
 */
  def main(args: Array[String]) {

    val sparkConf = new SparkConf(true)
      .set("spark.cassandra.connection.host", "127.0.0.1")
      .setMaster("local")
      .setAppName("spark-events")


    //val sc = new SparkContext("local[2]", "test", sparkConf)
    val sc = new SparkContext(sparkConf)
    val allevents = sc.cassandraTable("ks_events", "events")

    println("count: " + allevents.count)

    //allstarful.foreach(println(_))
    println("first: " + allevents.first)

    sc.stop()

  }
}
