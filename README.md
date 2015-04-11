# Spark Events

Example to generate aggregates from Cassandra tables and store it into other Cassandra tables.

##1 Create the keyspace and tables with CQL scripts found in events.cql in DevCenter or cqlsh

##2 Create the jar package
```
sbt package
```

##3 Run the job 
```
sbt run
```

##4 Or submit the job
```
./dse spark-submit --class EventsAggregations sparkevents_2.10-1.0.jar
```
