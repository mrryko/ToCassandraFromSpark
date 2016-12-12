# ToCassandraFromSpark
Create one more Spark Job, which listens to Kafka, parses published messages and saves them to Cassandra.

in terminal:

sudo service cassandra start

cqlsh

CREATE KEYSPACE demoCassandra WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};

use demoCassandra;

create table testSpark(countryCode text primary key, numberOfWords int);
