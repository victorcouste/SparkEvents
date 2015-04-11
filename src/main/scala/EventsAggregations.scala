
import org.apache.spark.SparkContext._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.joda.time.DateTime

object EventsAggregations  {

  /*
 * This is the entry point for the application
 */
  def main(args: Array[String]) {

    val sparkConf = new SparkConf(true)
      .set("spark.cassandra.connection.host", "127.0.0.1")
      .setMaster("local")
      .setAppName("spark-events")

    val sc = new SparkContext(sparkConf)
    val sqlc = new CassandraSQLContext(sc)


    //  --------- GROUP BY WITH SPARK ------------------------------------

    sc.cassandraTable("ks_events", "events").select("evt_id","evt_veh","evt_gts")
      .map( r => (r.get[String]("evt_id"),1))
      .reduceByKey(_ + _)
      //.foreach(println)
      .saveToCassandra("ks_events", "events_byevtid", SomeColumns("evt_id","nb_evts"))

    //  --------- GROUP BY WITH SQL ------------------------------------

    //sqlc.sql("select evt_id, count(*) from ks_events.events group by evt_id")
    //.toArray.foreach(println)
    //case class CountEvtID(evtid: String, count: Int)
    // .map( p => CountEvtID(p(0),p(1)))
    // OR  val .map( p => (p(0),p(1)))
    //.saveToCassandra("ks_events", "events_byevtid", SomeColumns("evt_id","nb_evts"))

    sqlc.sql("select evt_veh, count(*) from ks_events.events group by evt_veh")
      .map( p => (p(0),p(1)))
      .saveToCassandra("ks_events", "events_byevtveh", SomeColumns("evt_veh","nb_evts"))


    //println("count: " + eventsbyevtid.count)
    //println("first: " + eventsbyevtveh.first)

    sc.stop()

  }
}
