
import org.apache.spark.SparkContext._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.api.java
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
      .setAppName("spark-events-aggregations")

    val sc = new SparkContext(sparkConf)
    //val sqlc = new CassandraSQLContext(sc)

    val keyspace = "ks_events"

    //  --------- GROUP BY EVT_ID, EVT_EVH and EVT_GTS WITH SPARK ------------------------------------

    val allevents = sc.cassandraTable(keyspace, "events").select("evt_id","evt_veh","evt_gts")

    // ------ EVT ID ------------------------------------------

    allevents.map( r => (r.get[String]("evt_id"),1))
      .reduceByKey(_ + _)
      //.foreach(println)
      .saveToCassandra(keyspace, "events_byevtid", SomeColumns("evt_id","nb_evts"))

    // ------ EVT VEH ------------------------------------------

    allevents.map( r => (r.get[String]("evt_veh"),1))
      .reduceByKey(_ + _)
      //.foreach(println)
      .saveToCassandra(keyspace, "events_byevtveh", SomeColumns("evt_veh","nb_evts"))

    // ------ EVT ID+GTS DAY ------------------------------------------

    allevents.map( r => ((r.get[String]("evt_id"),r.get[DateTime]("evt_gts").dayOfMonth().roundFloorCopy()),1))
      .reduceByKey(_ + _)
      .map( r => (r._1._1,r._1._2,r._2))
      //.foreach(println)
      .saveToCassandra(keyspace, "events_bydayevtid", SomeColumns("evt_id","evt_day_gts","nb_evts"))

    // ------ EVT VEH+GTS DAY ------------------------------------------

    allevents.map( r => ((r.get[String]("evt_veh"),r.get[DateTime]("evt_gts").dayOfMonth().roundFloorCopy()),1))
      .reduceByKey(_ + _)
      .map( r => (r._1._1,r._1._2,r._2))
      //.foreach(println)
      .saveToCassandra(keyspace, "events_bydayevtveh", SomeColumns("evt_veh","evt_day_gts","nb_evts"))

    // ------ EVT GTS DAY ------------------------------------------

    //case class MyYearDay(evt_year_gts: String, evt_day_gts: DateTime, nb_evts: Int)

    allevents.map( r => ((r.get[DateTime]("evt_gts").year().getAsText(),r.get[DateTime]("evt_gts").dayOfMonth().roundFloorCopy()),1))
      .reduceByKey(_ + _)
      .map( r => (r._1._1,r._1._2,r._2))
      //.foreach(println)
    .saveToCassandra(keyspace, "events_byday", SomeColumns("evt_year_gts","evt_day_gts","nb_evts"))

    //  --------- GROUP BY WITH SQL ------------------------------------
    /*
    sqlc.sql("select evt_id, count(*) from ks_events.events group by evt_id")
    //.toArray.foreach(println)
    //case class CountEvtID(evtid: String, count: Int)
     .map( p => CountEvtID(p(0),p(1)))
    // OR  val .map( p => (p(0),p(1)))
    .saveToCassandra("ks_events", "events_byevtid", SomeColumns("evt_id","nb_evts"))

    sqlc.sql("select evt_veh, count(*) from ks_events.events group by evt_veh")
      .map( p => (p(0),p(1)))
      .saveToCassandra("ks_events", "events_byevtveh", SomeColumns("evt_veh","nb_evts"))
    */


    println("Nb events aggregated: " + allevents.count)
    //println("first: " + eventsbyevtveh.first)

    sc.stop()

  }
}
