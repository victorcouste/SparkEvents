# SparkEvents

Example to generate aggregates from Cassandra tables and store it into other Cassandra tables 

To create the keyspace and tables execute CQL scripts found in events.cql in DevCenter or cqlsh


To create the jar package execute
```
sbt package
```

Run the job with 
```
sbt run
```

Or submit the job with
```
./dse spark-submit --class EventsAggregations sparkevents_2.10-1.0.jar
```
